import csv
import re
import sys
import traceback
from collections import namedtuple
from io import StringIO

import pandas as pd
import sqlalchemy
from flask import current_app as flask_app
from sqlalchemy import DDL, text
from sqlalchemy_utils import functions as sa_functions

TableID = namedtuple('TableID', 'schema table')


class DBI:
    """
    Database Interface

    The main module which provides engine level access to the postgres
    database. This currently excludes any interaction with the
    database through the sqlalchemy ORM, but *should* include
    everything else.
    """

    def __init__(self, db):
        self.db = db

    @classmethod
    def parse_fully_qualified(cls, name):
        match = re.fullmatch(r'"(?P<schema>[a-zA-Z0-9_.]+)"\."(?P<table>[a-zA-Z0-9_.]+)"', name)
        if not match:
            raise ValueError(f'the supplied name ({name}) does not appear' ' to be well formed')

        groupdict = match.groupdict()
        return TableID(groupdict['schema'], groupdict['table'])

    @classmethod
    def to_fully_qualified(cls, table, schema=None):
        """Intelligently cast args to fully qualified table name.

        This is a "do what I mean" command and thus should handle
        various inputs in an intelligent way. The aim of this method
        is to reduce the need for code which checks for an applies
        quoting, which often seems to be required.
        """

        # already fully qualified
        if schema is None and table.count('"') == 4:
            return table

        if schema and table.count('"') == 4:
            try:
                table_id = cls.parse_fully_qualified(table)
            except ValueError as e:
                raise ValueError(
                    f'table ({table}) appears to be malformed' f'\ncaused by error: {e}'
                )
            if table_id.schema != cls.unquote(schema):
                raise ValueError(
                    f'table ({table}) already has a schema, and '
                    'the supplied schema ({schema}) does not match'
                )
            table_uq = table_id.table
            schema_uq = table_id.schema
        else:
            table_uq = cls.unquote(table)
            schema_uq = cls.unquote(schema)
        return f'"{schema_uq}"."{table_uq}"'

    @classmethod
    def quote(cls, name):
        """Quote a name according to postgres quoting rules.

        If the name seems to already be quoted, this returns name unchanged.
        """
        if name[0] == name[-1] == '"':
            return name
        return f'"{name}"'

    @classmethod
    def unquote(cls, name):
        """Unquote a name according to postgres quoting rules.

        If the name seems to be already unquoted, this returns name unchanged.
        """
        if name[0] == name[-1] == '"':
            return name[1:-1]
        return name

    def create_schema(self, name):
        stmt = f'CREATE SCHEMA IF NOT EXISTS "{name}"'
        self.execute_statement(stmt)

    def destroy_db(self):
        sa_functions.drop_database(self.db.engine.url)
        self.db.engine.dispose()

    def df_to_table(self, df, schema, table):
        df.to_sql(name=table, con=self.db.engine, schema=schema, if_exists='append')

    def df_to_table_bulk(self, df, fq_table_name, columns=None):
        """bulk insert dataframe (2.5 times faster then df_to_table)

        :param df: dataframe to insert. The index will be ignored.
        :param fq_table_name: fully qualified table name ("schema"."table")
        :param columns: dataframe columns to insert into the table. If all dataframe columns
            exactly match (also same order) the table columns, this can be left empty.
        """
        connection = self.db.engine.raw_connection()
        cur = connection.cursor()
        output = StringIO()
        df.to_csv(
            output,
            sep='\t',
            header=False,
            index=False,
            na_rep='None',
            encoding='utf-8',
            quoting=csv.QUOTE_NONE,
            quotechar='',
            escapechar='\\',
            columns=columns,
        )
        output.seek(0)
        cur.copy_from(output, fq_table_name, null='None', sep='\t', columns=columns)
        connection.commit()
        cur.close()

    def drop_schema(self, name):
        stmt = 'DROP SCHEMA IF EXISTS "{}" CASCADE'.format(name)
        self.execute_statement(stmt)

    def drop_table(self, fq_name):
        stmt = 'DROP TABLE IF EXISTS {} CASCADE'.format(fq_name)
        self.execute_statement(stmt)

    def drop_materialized_view(self, fq_name):
        stmt = 'DROP MATERIALIZED VIEW IF EXISTS {} CASCADE'.format(fq_name)
        self.execute_statement(stmt)

    def drop_sequence(self, fq_name):
        stmt = 'DROP SEQUENCE IF EXISTS {} CASCADE'.format(fq_name)
        self.execute_statement(stmt)

    def append_table(self, source_table, target_table, drop_source=True):
        stmt = f"""
               INSERT INTO {target_table} (
                 SELECT * FROM {source_table}
               );
           """
        self.execute_statement(stmt)
        if drop_source:
            self.drop_table(source_table)

    def execute_query(self, query, data=None, df=False, raise_if_fail=False):
        result_set = self.execute_statement(query, data, raise_if_fail)
        rows = result_set.fetchall()
        if df:
            return pd.DataFrame(rows, columns=result_set.keys())
        return rows

    def execute_statement(self, stmt, data=None, raise_if_fail=False):
        connection = self.db.engine.connect()
        transaction = connection.begin()
        try:
            status = connection.execute(text(stmt), data)
            transaction.commit()
            connection.close()
            return status
        except sqlalchemy.exc.ProgrammingError as err:
            transaction.rollback()
            flask_app.logger.error("Execute statement error:")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            if raise_if_fail:
                raise err
            connection.close()

    def recreate_schema(self, name):
        self.drop_schema(name)
        self.create_schema(name)

    def delete_public_tables(self):
        stmt = """
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' and tablename <> 'spatial_ref_sys') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """
        self.execute_statement(stmt)

    def create_index(self, index, table_name, index_name=None):
        index_name = index_name if index_name else '{}_index'.format(table_name)
        index_str = '(' + ','.join(index) + ')'
        sql = f'create index if not exists {index_name} on {table_name} {index_str}'
        self.execute_statement(sql, data=index)

    def insert_data(self, df, table_name):
        if len(df):
            sql = f'''
                insert into {table_name} ({','.join(df.columns)})
                values ({','.join(['%s' for i in range(len(df.columns))])})
                on conflict do nothing
            '''
            for col in df.columns:
                if df[col].dtype == '<M8[ns]':
                    df[col] = df[col].map(lambda x: None if pd.isnull(x) else x.isoformat())
            self.execute_statement(sql, df.values.tolist(), raise_if_fail=True)

    def schema_exists(self, name):
        stmt = f"""
          SELECT schema_name
          FROM information_schema.schemata
          WHERE schema_name = '{name}'
        """
        res = self.execute_statement(stmt)
        rows = res.fetchall()
        return len(rows) > 0

    def dsv_buffer_to_table(
        self,
        csv_buffer,
        fq_table_name,
        columns,
        has_header=False,
        null='',
        sep='\t',
        quote=None,
        encoding=None,
    ):
        connection = self.db.engine.raw_connection()
        cursor = connection.cursor()
        sql = self._get_sql_copy_statement(
            fq_table_name, columns, has_header, sep, null, quote, encoding
        )
        try:
            cursor.copy_expert(sql, csv_buffer)
            connection.commit()
        except sqlalchemy.exc.ProgrammingError:
            flask_app.logger.error("DSV to table error:")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
        cursor.close()
        connection.close()

    @staticmethod
    def _get_sql_copy_statement(table, columns, has_header, delimiter, null_value, quote, encoding):
        sql = 'COPY {}'.format(table)
        if columns:
            sql += ' ({})'.format(','.join(columns))
        sql += ' FROM STDIN WITH CSV'
        if has_header:
            sql += ' HEADER'
        if delimiter:
            sql += " DELIMITER E'{}'".format(delimiter)
        if null_value:
            sql += " null as '{}'".format(null_value)
        if quote:
            sql += " QUOTE \'{}\'".format(quote)
        if encoding:
            sql += f" ENCODING '{encoding}'"
        return sql

    def rename_table(self, schema, table_name, new_table_name):
        stmt = f"""
            select indexname
            from pg_indexes
            where tablename = '{table_name}' and schemaname = '{schema}';
        """
        indices = self.execute_query(stmt, df=False)
        for index in indices:
            stmt = f"""
                alter index "{schema}"."{index[0]}"
                rename to "{index[0].replace(table_name, new_table_name)}";
            """
            self.execute_statement(stmt)
        stmt = f"""
            SELECT relname FROM pg_class c
            join pg_namespace nsp on nsp.oid = c.relnamespace
            WHERE c.relkind = 'S'
            and relname = '{table_name}_id_seq' and nspname = '{schema}';
        """
        sequences = self.execute_query(stmt, df=False)
        if sequences:
            stmt = f"""
                alter sequence "{schema}"."{sequences[0][0]}"
                rename to "{sequences[0][0].replace(table_name, new_table_name)}";
            """
            self.execute_statement(stmt)
        stmt = f"""
            alter table "{schema}"."{table_name}" rename to "{new_table_name}";
        """
        self.execute_statement(stmt)

    def table_exists(self, schema, table_name, materialized_view=False):
        query = f"""
         SELECT EXISTS (
            SELECT 1
               FROM   {'information_schema.tables' if not materialized_view else 'pg_matviews'}
               WHERE  {'table_schema' if not materialized_view else 'schemaname'} = '{schema}'
               AND    {'table_name' if not materialized_view else 'matviewname'} = '{table_name}'
         );
        """
        return list(self.execute_query(query))[0][0]

    def refresh_materialised_view(self, fq_view_name):
        self.execute_statement(
            DDL(
                f"""
            REFRESH MATERIALIZED VIEW {fq_view_name};
        """
            )
        )

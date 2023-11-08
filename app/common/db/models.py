import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, event, text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import load_only, relationship
from sqlalchemy.sql import ClauseElement

db = SQLAlchemy()
sql_alchemy = db

# aliases
_sa = sql_alchemy
_col = db.Column
_text = db.Text
_int = db.Integer
_dt = db.DateTime
_bigint = _sa.BigInteger
_bool = db.Boolean
_num = db.Numeric
_array = _sa.ARRAY
_date = _sa.Date
_enum = _sa.Enum
_float = _sa.Float
_decimal = _num
_uuid = UUID
_table = db.Table
_foreign_key = db.ForeignKey
_relationship = relationship
_check = CheckConstraint
_unique = UniqueConstraint


class BaseModel(db.Model):
    __abstract__ = True

    def save(self):
        _sa.session.add(self)
        _sa.session.commit()

    def delete(self):
        _sa.session.delete(self)
        _sa.session.commit()

    @classmethod
    def create_table(cls):
        db.metadata.create_all(bind=db.engine, tables=[cls.__table__], checkfirst=True)

    @classmethod
    def drop_table(cls):
        cls.__table__.drop(db.engine, checkfirst=True)

    @classmethod
    def recreate_table(cls):
        cls.drop_table()
        cls.create_table()

    @classmethod
    def get_schema(cls):
        if 'schema' in cls.__table_args__:
            return cls.__table_args__['schema']
        else:
            return 'public'

    @classmethod
    def get_fq_table_name(cls):
        return f'"{cls.get_schema()}"."{cls.__tablename__}"'

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        """
        Creates a new object or returns existing.

        Example:
            object, created = Model.get_or_create(a=1, b=2, defaults=dict(c=3))

        :param defaults: (dictionary) of fields that should be saved on new instance
        :param kwargs: fields to query for an object
        :return: (Object, boolean) (Object, created)
        """
        instance = _sa.session.query(cls).filter_by(**kwargs).first()
        _sa.session.close()
        if instance:
            return instance, False
        else:
            params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
            params.update(defaults or {})
            instance = cls(**params)
            instance.save()
            _sa.session.commit()
            _sa.session.close()
            return instance, True

    @classmethod
    def get_dataframe(cls, columns=None):
        query = cls.query
        if columns:
            query = query.options(load_only(*columns))
        return pd.read_sql(query.statement, db.engine, index_col=cls.id.description)


def create_schemas(*args, **kwargs):
    try:
        from app.db.models import get_schemas
    except ImportError:
        schemas = ['admin']
    else:
        schemas = get_schemas()

    for schema in schemas:
        with _sa.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    _sa.session.commit()


event.listen(BaseModel.metadata, 'before_create', create_schemas)


class HawkUsers(BaseModel):
    __tablename__ = 'hawk_users'
    __table_args__ = {'schema': 'public'}

    id = _col(_text, primary_key=True)
    key = _col(_text)
    scope = _col(_array(_text))
    description = _col(_text)

    @classmethod
    def get_client_key(cls, client_id):
        query = _sa.session.query(cls.key).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def get_client_scope(cls, client_id):
        query = _sa.session.query(cls.scope).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def add_user(cls, client_id, client_key, client_scope, description):
        cls.get_or_create(
            id=client_id,
            defaults={'key': client_key, 'scope': client_scope, 'description': description},
        )

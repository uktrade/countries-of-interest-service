import json
import re
import urllib.parse

import requests
from flask import current_app as flask_app
from mohawk import Sender

from app.config import constants
from app.db import db_utils
from app.db.db_utils import execute_statement
from app.db.models import db
from app.db.models.external import DatahubCompany, DatahubContact, ExportWins
from app.db.models.internal import (
    CountriesAndSectorsInterest,
    CountriesAndSectorsInterestMatched,
    CountriesAndSectorsInterestTemp,
)


class Task:

    name = constants.Task.COMPANY_MATCHING.value
    valid_email = re.compile(r"[^@]+@[^@]+\.[^@]+")

    def __init__(self, **kwargs):
        pass

    def __call__(self):
        self._company_matching()
        return {
            'status': 200,
            'task': self.name,
        }

    def _company_matching(self):
        connection = db.engine.raw_connection()
        cursor = self._fetch_companies(connection)
        self._match_and_store_results(cursor)
        cursor.close()
        connection.close()
        self._populate_csi_matched_table_and_swap()

    def _fetch_companies(self, connection):
        cursor = connection.cursor(name='fetch_companies')
        query = f'''
            (SELECT distinct
                csi.service_company_id,
                company.company_name,
                contact.email,
                NULLIF(company.reference_code, '') AS cdms_ref,
                company.postcode,
                company.companies_house_id,
                'dit.datahub' as source,
                company.modified_on as datetime
            FROM {CountriesAndSectorsInterestTemp.get_fq_table_name()} csi
            INNER JOIN {DatahubCompany.get_fq_table_name()} company
                on csi.service_company_id = company.datahub_company_id::text
            LEFT JOIN
                (select * from {DatahubContact.get_fq_table_name()}
                where email is not null) contact
                using (datahub_company_id)
            WHERE service = 'datahub')
            UNION
            (SELECT distinct
                csi.source_id,
                ew.company_name,
                ew.contact_email_address,
                NULLIF(ew.export_wins_company_id, '') AS cdms_ref,
                null as postcode,
                null as copmanies_house_id,
                'dit.export-wins' as source,
                ew.created_on::timestamp
            FROM {CountriesAndSectorsInterestTemp.get_fq_table_name()} csi
            INNER JOIN {ExportWins.get_fq_table_name()} ew
                on csi.source_id = ew.export_wins_id::text
            WHERE service = 'export_wins')
        '''
        cursor.execute(query)
        return cursor

    def _match_and_store_results(self, cursor):
        stmt = """
            DROP TABLE IF EXISTS company_matching;
            CREATE TABLE company_matching (
                id text,
                match_id int,
                similarity text
            );
        """
        execute_statement(stmt)

        for request in self._build_request(cursor):
            status_code, data = self._post_request(
                params='',
                endpoint=urllib.parse.urljoin(
                    flask_app.config['cms']['base_url'], '/api/v1/company/match/'
                ),
                json_query=request,
            )
            stmt = f"""
                INSERT INTO company_matching (
                    id,
                    match_id,
                    similarity
                )
                SELECT distinct on (id)
                    id,
                    match_id,
                    similarity
                FROM json_populate_recordset(null::company_matching, %s);
            """
            execute_statement(stmt, data=(json.dumps(data['matches']),))

    def _build_request(self, cursor, batch_size=100000):
        batch_count = 0
        while True:
            descriptions = []
            request = {'descriptions': descriptions}
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            start_count = batch_count * batch_size
            end_count = batch_count * batch_size + len(rows)
            flask_app.logger.info(f"matching company {f'{start_count}-{end_count}'}")
            for row in rows:
                description = {
                    'id': str(row[0]),
                    'datetime': row[7].strftime("%Y-%m-%d %H:%M:%S"),
                    'source': row[6],
                }
                if row[1]:
                    description['company_name'] = row[1]
                if row[2] and self.valid_email.match(row[2]):
                    description['contact_email'] = row[2]
                if row[3]:
                    description['cdms_ref'] = str(row[3])
                if row[4]:
                    description['postcode'] = row[4]
                if row[5] and len(row[5]) == 8:
                    description['companies_house_id'] = row[5]
                descriptions.append(description)
            yield request
            batch_count += 1

    def _post_request(self, params, endpoint, json_query):
        json_data = json.dumps(json_query)
        url = f'{endpoint}?{params}'.rstrip('?')

        def get_sender():
            return Sender(
                credentials={
                    'id': flask_app.config['access_control']['hawk_client_id'],
                    'key': flask_app.config['access_control']['hawk_client_key'],
                    'algorithm': 'sha256',
                },
                url=url,
                method='POST',
                content=json_data,
                content_type='application/json',
            )

        res = requests.post(
            url, headers={'Authorization': get_sender().request_header}, json=json_query,
        )
        status_code = res.status_code
        try:
            data = res.json()
        except ValueError:
            data = None
        return status_code, data

    def _populate_csi_matched_table_and_swap(self):
        CountriesAndSectorsInterestMatched.drop_table()
        CountriesAndSectorsInterestMatched.create_table()
        stmt = f"""
            INSERT INTO {CountriesAndSectorsInterestMatched.get_fq_table_name()} (
                service_company_id,
                company_match_id,
                country,
                sector,
                type,
                service,
                source,
                source_id,
                timestamp
            )
            SELECT
                service_company_id,
                match_id,
                country,
                sector,
                type,
                service,
                source,
                source_id,
                timestamp
            FROM ((SELECT
                csi.id,
                csi.service_company_id,
                dh_cm.match_id,
                csi.country,
                csi.sector,
                csi.type,
                csi.service,
                csi.source,
                csi.source_id,
                csi.timestamp
            FROM {CountriesAndSectorsInterestTemp.get_fq_table_name()} csi
            LEFT JOIN (
                select distinct id, match_id from company_matching
            ) dh_cm on dh_cm.id = csi.service_company_id
            WHERE csi.service = 'datahub')
            UNION
            (SELECT
                csi.id,
                csi.service_company_id,
                ew_cm.match_id,
                csi.country,
                csi.sector,
                csi.type,
                csi.service,
                csi.source,
                csi.source_id,
                csi.timestamp
            FROM {CountriesAndSectorsInterestTemp.get_fq_table_name()} csi
            LEFT JOIN (
                select distinct id, match_id from company_matching
            ) ew_cm on ew_cm.id = csi.source_id and csi.service = 'export_wins'
            WHERE csi.service = 'export_wins')) SQ ORDER BY id;
        """
        execute_statement(stmt)
        CountriesAndSectorsInterest.drop_table()
        db_utils.rename_table(
            CountriesAndSectorsInterestMatched.__tablename__,
            CountriesAndSectorsInterest.__tablename__,
        )

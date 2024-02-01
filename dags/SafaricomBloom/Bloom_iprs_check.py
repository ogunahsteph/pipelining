from __future__ import print_function

import logging
import os
import sys
import datetime
import time
import pendulum
import json
import numpy as np
import pandas as pd
from airflow import DAG
from fuzzywuzzy import fuzz
from airflow.models import Variable
from psycopg2 import IntegrityError
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from utils.creditinfo_api import get_token, get_results
from utils.common import on_failure

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-bloom')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='monitoring')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None
}

local_tz = pendulum.timezone("Africa/Nairobi")


with DAG(
        'Bloom_IPRS_check',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,  # '30 13 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2022, 6, 17, 14, 00, tzinfo=local_tz),
        tags=['iprs_check'],
        description='Check iprs information for national ids',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    DAG is triggered manually to refresh IDM data
    pipeline uses endpoint provided by IT
    """

    def do_iprs_check(df: pd.DataFrame) -> pd.DataFrame:
        """
        Uses provided API to refresh IDM data

        param df: A pandas dataframe containing national ids to be refreshed
        """
        for index, row in df.iterrows():
            time.sleep(5)
            tken_details = get_token(
                str(row['national_id']).strip().replace('.0', '').replace('.', '')
                , env='live', action='iprs'
            )
            retries = 2
            while tken_details.status_code == 429 and retries > 0:
                time.sleep(2)
                tken_details = get_token(
                    str(row['national_id']).strip().replace('.0', '').replace('.', '')
                    , env='live', action='iprs'
                )
                retries -= 1

            try:
                res = get_results(token_id=json.loads(tken_details.text)['Token'], env='live')
                while res.status_code == 202:
                    time.sleep(2)
                    res = get_results(token_id=json.loads(tken_details.text)['Token'], env='live')

                df.loc[index, 'raw_response'] = res.text
                consolidated_report = None
                if 'response' in json.loads(res.text)['Data']['response']['ConsolidatedReport'].keys():
                    consolidated_report = json.loads(res.text)['Data']['response']['ConsolidatedReport']['response']
                elif 'SerialNumber' in json.loads(res.text)['Data']['response']['Extract'].keys():
                    consolidated_report = json.loads(res.text)['Data']['response']['Extract']

                if consolidated_report is not None:
                    df.loc[index, 'is_refreshed'] = 1
                    df.loc[index, 'first_name'] = consolidated_report.get('FirstName', np.NAN)
                    df.loc[index, 'surname'] = consolidated_report.get('Surname', np.NAN)
                    df.loc[index, 'other_name'] = consolidated_report.get('OtherName', np.NAN)
                    df.loc[index, 'date_of_issue'] = consolidated_report.get('DateOfIssue', np.NAN)
                    df.loc[index, 'serial_number'] = consolidated_report.get('SerialNumber', np.NAN)
                else:
                    df.loc[index, 'is_refreshed'] = 1
                    df.loc[index, 'first_name'] = np.NAN
                    df.loc[index, 'surname'] = np.NAN
                    df.loc[index, 'other_name'] = np.NAN
                    df.loc[index, 'date_of_issue'] = np.NAN
                    df.loc[index, 'serial_number'] = np.NAN

            except json.JSONDecodeError as e: # Token was not retrieved
                continue
            except KeyError as e:
                # res = get_results(token_id=json.loads(tken_details.text)['Token'], env='live')
                logging.warning(str(e))
                logging.warning(f'{row}')
                continue
            except IntegrityError as e:
                logging.warning(str(e))
                continue
            except Exception as e:
                logging.warning(f'{str(e), e.__class__.__name__}')
                continue

        if df[df['is_refreshed'] == 1].shape[0] > 0:
            return df[df['is_refreshed'] == 1]


    def store_refreshed_data(refreshed_data: pd.DataFrame) -> None:
        """
        stores refreshed CRB data in the warehouse

        param sent_sms: A pandas dataframe containing only those national ids that have been refreshed
        """
        refreshed_data['national_id'] = refreshed_data['national_id'].apply(
            lambda x: str(x).strip().replace('.0', '')
        )
        refreshed_data['menu'] = 'iprs'
        airflow_hook.insert_rows(
            table='bloom_pipeline.iprs_idm_logs',
            target_fields=[
                'national_id', 'menu', 'raw_response'
            ],
            replace=False,
            rows=tuple(refreshed_data[[
                'national_id', 'menu', 'raw_response'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=200
        )
        warehouse_hook.insert_rows(
            table='bloomlive.iprs_kyc',
            target_fields=[
                'national_id', 'first_name', 'surname', 'other_name', 'date_of_issue', 'serial_number', 'raw_response'
            ],
            replace=False,
            rows=tuple(refreshed_data[[
                'national_id', 'first_name', 'surname', 'other_name', 'date_of_issue', 'serial_number', 'raw_response'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=200
        )

    def get_and_refresh_national_ids(**context):
        """
        Iterates over national ids and refreshes their CRB data
        """
        total_national_ids = warehouse_hook.get_pandas_df(
            sql="""
                select count(distinct(national_id)) as total_national_ids from bloomlive.client_summary_view csv
                where not is_iprs_checked and national_id ~ '^[0-9\.]+$' and not length(national_id) > 8
            """
        ).iloc[0]['total_national_ids'].astype('int')

        chunksize = 5
        refreshed_national_ids = []
        for num in range(0, total_national_ids, chunksize):
            national_ids_to_refresh = warehouse_hook.get_pandas_df(
                sql="""
                    select distinct(national_id) from bloomlive.client_summary_view csv
                    where not is_iprs_checked and national_id ~ '^[0-9\.]+$' and not length(national_id) > 8
                    order by national_id asc offset %(start_index)s limit %(chunksize)s
                """,
                parameters={'start_index': num, 'chunksize': chunksize}
            ).drop_duplicates(subset=['national_id'])

            national_ids_to_refresh = national_ids_to_refresh[~national_ids_to_refresh['national_id'].str.contains('|'.join(['-', '/', '_']))]
            national_ids_to_refresh['national_id'] = national_ids_to_refresh['national_id'].apply(
                lambda x: str(x).strip().replace('.0', '') if not pd.isnull(x) else x
            )

            def allCharactersSame(s):
                n = len(s)
                for i in range(1, n):
                    if s[i] != s[0]:
                        return False
                return True

            national_ids_to_refresh.copy().apply(
                lambda x: national_ids_to_refresh.drop(index=x.name, inplace=True) if allCharactersSame(x['national_id']) and '0' in x['national_id'] else x,
                axis=1
            )

            if national_ids_to_refresh.shape[0] > 1:
                national_ids_to_refresh['is_refreshed'] = 0
                national_ids_to_refresh['first_name'] = np.NAN
                national_ids_to_refresh['surname'] = np.NAN
                national_ids_to_refresh['other_name'] = np.NAN
                national_ids_to_refresh['date_of_issue'] = np.NAN
                national_ids_to_refresh['serial_number'] = np.NAN
                national_ids_to_refresh['raw_response'] = np.NAN

                already_existing = warehouse_hook.get_pandas_df(
                    sql="""
                        select national_id::varchar from bloomlive.iprs_kyc
                        where national_id in %(national_ids)s
                    """,
                    parameters={'national_ids': tuple(national_ids_to_refresh['national_id'].tolist())}
                )
                national_ids_to_refresh = national_ids_to_refresh[~national_ids_to_refresh['national_id'].isin(already_existing['national_id'])]

                national_ids_to_refresh.reset_index(drop=True, inplace=True)

                results = do_iprs_check(national_ids_to_refresh)

                if results is not None:
                    refreshed_national_ids.append(results)

            # store refreshed national ids
            try:
                store_refreshed_data(pd.concat(refreshed_national_ids))
                send_ms_teams_notification = MSTeamsWebhookOperator(
                    task_id='send_ms_teams_notification', trigger_rule="all_done",
                    http_conn_id='msteams_webhook_url',
                    message="""Bloom IPRS refresh""",
                    subtitle=f"{len(pd.concat(refreshed_national_ids))} national ids have been refreshed on IPRS"
                )
                send_ms_teams_notification.execute(context)
                refreshed_national_ids = []
            except ValueError as e:
                logging.warning("results is an empty list")

    def get_fuzzy_score_updates(clients: pd.DataFrame, table_name) -> list:
        clients["fuzzy_match_score"] = clients.apply(
            lambda row: fuzz.token_set_ratio(row['customer_name'], row['iprs_name']), axis=1)

        updates = []
        for i, r in clients.iterrows():
            updates.append(
                f"update bloomlive.{table_name} set fuzzy_match_score = {r['fuzzy_match_score']} where surrogate_id = {r['surrogate_id']}")

        return updates

    def get_fuzzy_match_scores_bloom_client_dimension(**context):
        clients = warehouse_hook.get_pandas_df(
            sql="""
                select 
                    cd.surrogate_id, lower(concat(cd.first_name, ' ', cd.middle_name, ' ', cd.last_name)) as customer_name,
                    lower(concat(ik.first_name, ' ', ik.other_name, ' ', ik.surname)) as iprs_name
                from bloomlive.client_dimension cd 
                left join bloomlive.iprs_kyc ik on trim(cd.national_id)::varchar = trim(ik.national_id)::varchar
                where cd.fuzzy_match_score is null and ik.serial_number is not null
                limit 100
            """
        )
        if clients.shape[0] > 0:
            updates = get_fuzzy_score_updates(
                clients=clients,
                table_name='client_dimension'
            )

            warehouse_hook.run(sql=updates)


    t1 = PythonOperator(
        task_id='get_and_refresh_national_ids',
        python_callable=get_and_refresh_national_ids,
        retries=0
    )
    t2 = PythonOperator(
        task_id='get_fuzzy_match_scores_bloom_client_dimension',
        python_callable=get_fuzzy_match_scores_bloom_client_dimension,
        retries=0
    )

    t1 >> t2

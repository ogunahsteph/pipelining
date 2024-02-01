import os
import sys
import math
import logging
import datetime
import requests
import pendulum
import pandas as pd
from io import StringIO
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.aws_api import get_objects, save_file_to_s3
from scoring_scripts.interswitch_uganda import get_scoring_results

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None
}

local_tz = pendulum.timezone("Africa/Nairobi")


with DAG(
        'scoring_pipeline_interswitch_uganda',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 6, 8, 12, 00, tzinfo=local_tz),
        tags=['scoring_pipeline'],
        description='score interswitch uganda clients',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Retrieves interswitch scoring data, passes the data through a scoring script, stores generated limits and finally returns the limits to IT endpoint

    #### Actions
    <ol>
    <li>Extract client data from server 167.172.162.10 file dumps</li>
    <li>Store file dumps into warehouse</li>
    <li>Pass fetched data to a scoring script and execute the script</li>
    <li>stores generated limits</li>
    <li>passes generated limits via an API to Engineering API endpoint</li>
    </ol>
    
    Pass "agent_id" parameter to fetch files for a specific agent. For example, to fetch files for agent 3is00362
    pass below configuration parameters.
    
    The parameters have to be in valid JSON. Use double quotes for keys and values
    ```
     {"agent_id": "3is00362"}
    ```
    
    If the no "agent_id" parameter is passed, the pipeline does execute scoring.
    
    """

    def combine_files(agent_id):
        files = get_objects(
            aws_access_key_id=Variable.get('AFSG_aws_access_key_id') if Variable.get('DEBUG') == 'FALSE' else None,
            aws_secret_access_key=Variable.get('AFSG_aws_secret_access_key')if Variable.get('DEBUG') == 'FALSE' else None,
            search=agent_id,
            bucket_name=Variable.get('ISWUG_aws_s3_bucket_name')
        )

        if len(files) > 0:
            combined = pd.concat([x['data'] for x in files], ignore_index=True)

            combined['Balance'] = combined['Balance'].apply(
                lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)
            combined['CreditAmount'] = combined['CreditAmount'].apply(
                lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)
            combined['DebitAmount'] = combined['DebitAmount'].apply(
                lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)

            combined.rename(columns={
                'Date': 'time_', 'Terminal': 'terminal', 'RequestRef': 'request_ref',
                'TranDesc': 'trxn_type', 'Biller': 'biller', 'Narration': 'narration',
                'DebitAmount': 'debit_amt', 'CreditAmount': 'credit_amt', 'Balance': 'balance', 'Status': 'status'
            }, inplace=True)


            combined.drop_duplicates(subset=combined.columns.tolist(), inplace=True)

            if combined.shape[0] > 0:
                csv_buffer = StringIO()
                combined.to_csv(path_or_buf=csv_buffer, index=False)
                save_file_to_s3(
                    s3_file_key=f"interswitch_uganda/scoring_data/{agent_id}_cleaned.csv",
                    bucket_name='afsg-ds-prod-postgresql-dwh-archive',
                    file_bytes=csv_buffer.getvalue()
                )
                return combined
            else:
                logging.warning(f'All files for {agent_id} are empty')

        return None

    def share_scoring_results(payload: dict):
        res = requests.post(
            url='https://iswug.asantefsg.com/iswug/api/v1/client/creditlimit',
            headers={'Content-Type': 'application/json'},
            json=payload,
            auth=requests.auth.HTTPBasicAuth(Variable.get('isw_uganda_client_id'),
                                             Variable.get('isw_uganda_client_secret')),
            verify=False
        )
        return res

    def pass_generated_limits_to_engineering(**context):
        agent_id = context['dag_run'].conf.get('agent_id', None)
        failure_reason = context['ti'].xcom_pull(task_ids='trigger_scoring', key='failure_reason')

        if failure_reason:
            payload = {
                "clientId": "AsanteDS538",
                "agentId": agent_id,
                "final_3_day_limit": int(0),
                "final_7_day_limit": int(0),
                "createdDate": str(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                'extra': {
                    'limit_reason_3_day': failure_reason,
                    'limit_reason_7_day': failure_reason
                }
            }
        else:
            rslt = warehouse_hook.get_pandas_df(
                sql="""
                select id, terminal as agent_id, 
                case 
                    when not is_qualified then 0 else final_3_day_limit
                end as final_3_day_limit,
                case 
                    when not is_qualified then 0 else final_7_day_limit
                end as final_7_day_limit,  
                tenure, is_qualified from interswitch_ug.scoring_results_view
                where lower(terminal) = %(agent_id)s
                """,
                parameters={'agent_id': str(agent_id).strip().lower()}
            )
            # tenures = {'3': 'final_3_day_limit', '7': 'final_7_day_limit'}
            rslt.iloc[0] = rslt.iloc[0].fillna(0)

            # -------------------------------
            payload = {
                "clientId": "AsanteDS538",
                "agentId": rslt.iloc[0]['agent_id'],
                "final_3_day_limit": int(rslt.iloc[0]['final_3_day_limit']),
                "final_7_day_limit": int(rslt.iloc[0]['final_7_day_limit']),
                "createdDate": str(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                'extra': {
                    'limit_reason_3_day': 'Success' if rslt.iloc[0]['is_qualified'] else 'Client does not pass business rules',
                    'limit_reason_7_day': 'Success' if rslt.iloc[0]['is_qualified'] else 'Client does not pass business rules'
                }
            }

        res = share_scoring_results(payload=payload)
        logging.warning(f'\n---------Payload---------\n{payload}')

        if res.status_code != 200:
            logging.warning(f'\n---------Response---------\n{res}')
            res.raise_for_status()
        else:
            logging.warning(f'\n---------Response--------\n{res.text}')


    def trigger_scoring(**context):
        agent_id = context['dag_run'].conf.get('agent_id', None)

        if agent_id is not None:
            raw_data = combine_files(agent_id)
            if raw_data is not None:
                failure_reason = get_scoring_results(raw_data=raw_data)
                context['ti'].xcom_push(key='failure_reason', value=failure_reason)
                return True
            else:
                raise pd.errors.EmptyDataError

        return False

    t1 = ShortCircuitOperator(
        task_id='trigger_scoring',
        python_callable=trigger_scoring,
        retries=0
    )
    t2 = PythonOperator(
        task_id='pass_generated_limits_to_engineering',
        provide_context=True,
        python_callable=pass_generated_limits_to_engineering
    )

    t1 >> t2
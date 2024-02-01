import os
import sys
import logging
import datetime
import pendulum
import requests
from airflow import DAG
from urllib import parse
from datetime import timedelta
from airflow.models import Variable
import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from scoring_scripts.remita_nigeria import get_scoring_results

remita_hook = MySqlHook(mysql_conn_id='remita_server', schema='remita_staging')
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
    'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'scoring_pipeline_remita',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,  # '* * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2022, 6, 8, 12, 00, tzinfo=local_tz),
        tags=['scoring_pipeline'],
        description='score remita clients',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
        ### DAG SUMMARY
        DAG is triggered by Digital team to score an Interswitch Uganda client
        
        
        Required parameters
        ```
         {
            "callback_url": "https://callback/url/endpoint",
            "bvn": "22142602171"
         }
        ```
    """


    def pass_generated_limits_to_engineering(**context):
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                      schema='afsg_ds_prod_postgresql_dwh')
        # scoring_response = context['ti'].xcom_pull(task_ids='trigger_scoring', key='scoring_results')

        bvn = context['dag_run'].conf.get('bvn')
        callback_url = context['dag_run'].conf.get('callback_url', None)

        scoring_results = warehouse_hook.get_pandas_df(
            sql="""
                select id, bvn_no, payday_limit_to_share, payday_final_is_qualified, 
                final_is_qualified,rules_summary_narration,payday_rules_summary_narration,is_3_months_qualified,
                is_6_months_qualified,final_allocated_limit_3_months,final_allocated_limit_6_months, 
                payroll_6_months_limit_to_share, payroll_3_months_limit_to_share, final_is_qualified_3_months,
                final_is_qualified_6_months, payroll_6_months_tenure, payroll_3_months_tenure
            from remita.scoring_results_remita_view where bvn_no = %(bvn)s
            """,
            parameters={'bvn': str(bvn)}
        )
        phone_numbers = remita_hook.get_pandas_df(
            sql="""
                with rnked as (
                select Bvn as BvnNo, MobileNumber as PhoneNo, rank() over (partition by Bvn order by CreatedDate desc) rnk
                from remita_staging.Customers where Bvn = %(bvn)s
            ) select * from rnked where rnk = 1 limit 1
            """,
            parameters={'bvn': str(bvn)}
        ).drop_duplicates(subset=['PhoneNo'])

        scoring_results = scoring_results.merge(
            phone_numbers, left_on='bvn_no', right_on='BvnNo', how='left'
        ).drop(columns=['BvnNo'])

        # print(type(scoring_results.iloc[0]['final_is_qualified']))
        # print(type(scoring_results.iloc[0]['payday_final_is_qualified']))

        if (scoring_results.iloc[0]['final_is_qualified_3_months'] == True) or \
                (scoring_results.iloc[0]['final_is_qualified_6_months'] == True) or \
                (scoring_results.iloc[0]['payday_final_is_qualified'] == True):

            payroll_6_month_limit = scoring_results.iloc[0]['payroll_6_months_limit_to_share']

            payroll_3_month_limit = scoring_results.iloc[0]['payroll_3_months_limit_to_share']

            payroll_1_month_limit = scoring_results.iloc[0]["payday_limit_to_share"]

            if not scoring_results.iloc[0]["final_is_qualified_6_months"] == True:
                payroll_6_month_limit = 0

            else:
                payroll_6_month_limit = payroll_6_month_limit

            if not scoring_results.iloc[0]["final_is_qualified_3_months"] == True:
                payroll_3_month_limit = 0
            else:
                payroll_3_month_limit = payroll_3_month_limit

            if not scoring_results.iloc[0]["payday_final_is_qualified"] == True:
                payroll_1_month_limit = 0
            else:
                payroll_1_month_limit = payroll_1_month_limit

            payload = {
                "clientId": "AsanteDs0521",
                "phoneNo": str(scoring_results.iloc[0]['PhoneNo']).replace('.0', ''),
                "bvn": str(scoring_results.iloc[0]['bvn_no']).replace('.0', ''),
                "is_6_months_qualified": bool(scoring_results.iloc[0]['final_is_qualified_6_months']),
                "is_3_months_qualified": bool(scoring_results.iloc[0]['final_is_qualified_3_months']),
                "is_1_months_qualified": bool(scoring_results.iloc[0]['payday_final_is_qualified']),
                "payroll_6_month_limit": payroll_6_month_limit,
                "payroll_3_month_limit": payroll_3_month_limit,
                "payroll_1_month_limit": payroll_1_month_limit,
                "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                "extras": {
                    "payroll_6_month_limit_reason": scoring_results.iloc[0]['rules_summary_narration'],
                    "payroll_3_month_limit_reason": scoring_results.iloc[0]['rules_summary_narration'],
                    'payroll_1_month_limit_reason': scoring_results.iloc[0]['payday_rules_summary_narration'],
                }
            }
        else:
            payload = {
                "clientId": "AsanteDs0521",
                "phoneNo": str(-1),
                "bvn": str(bvn),
                "is_6_months_qualified": False,
                "is_3_months_qualified": False,
                "is_1_months_qualified": False,
                "payroll_6_month_limit": -1,
                "payroll_3_month_limit": -1,
                "payroll_1_month_limit": -1,
                "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                "extras": {
                    "payroll_6_month_limit_reason": scoring_results.iloc[0]['rules_summary_narration'],
                    "payroll_3_month_limit_reason": scoring_results.iloc[0]['rules_summary_narration'],
                    "payroll_1_month_limit_reason": scoring_results.iloc[0]['payday_rules_summary_narration'],
                }
            }

        # Share Limits

        res = requests.post(
            url=callback_url,
            headers={'Content-Type': 'application/json'},
            json=payload,
            auth=requests.auth.HTTPBasicAuth(Variable.get('remita_api_username'), Variable.get('remita_api_password')),
        )

        logging.warning(
            f'\n-------------------- Payload --------------------\n {payload}\n--------------- Response ---------------\n status_code: {res.status_code}\n {res.text}')


    def trigger_scoring(**context):
        client_bvn = context['dag_run'].conf.get('bvn', None)

        if client_bvn is not None:
            context['ti'].xcom_push(key='scoring_results', value=get_scoring_results(bvn=client_bvn))
            return True

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

    # t1

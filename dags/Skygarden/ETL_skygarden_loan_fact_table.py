import os
import sys
import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, el_loans_fact_table


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_skygarden_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2021, 9, 29, 5, 00, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into skygarden loans fact table',
        user_defined_macros=default_args
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.skygarden.loans_fact_table following SCD type 2
    DAG is set to run daily at 5 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_loan</li>
    <li>Check for existing loan instances in ubuntu.skygarden.loans_fact_table</li>
    <li>Update existing instances in ubuntu.skygarden.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.skygarden.loans_fact_table</li>
    </ol>
    """

    def extract_load_loans_fact_table(**context) -> None:
        el_loans_fact_table(mifos_tenant='tanda', warehouse_schema='skygarden')

    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='skygarden',
            filters=None
        )

    t1 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )

    # TASKS
    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )

    t1 >> t2

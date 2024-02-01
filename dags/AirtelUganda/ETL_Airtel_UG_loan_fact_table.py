import os
import sys
import pendulum
import datetime
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
        'ETL_Airtel_UG_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2021, 10, 4, 6, 30, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into airtel_ug loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.airtel_uganda_device_financing.loans_fact_table following SCD type 2
    DAG is triggered automatically after dimensions loading

    #### Actions
    <ol>
    <li>Extract from mifostenant-devices.m_loan</li>
    <li>Check for existing loan instances in ubuntu.airtel_uganda_device_financing.loans_fact_table</li>
    <li>Update existing instances in ubuntu.airtel_uganda_device_financing.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.airtel_uganda_device_financing.loans_fact_table</li>
    </ol>
    """

    def extract_load_loans_fact_table(**context) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse
        :param context: dictionary of predefined values
        :return: None
        """

        mifos_tenant = 'devices'
        warehouse_schema = 'airtel_uganda_device_financing'

        el_loans_fact_table(mifos_tenant=mifos_tenant, warehouse_schema=warehouse_schema)


    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='airtel_uganda_device_financing',
            filters=None
        )

    # TASKS
    t1 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )

    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )

    t1 >> t2
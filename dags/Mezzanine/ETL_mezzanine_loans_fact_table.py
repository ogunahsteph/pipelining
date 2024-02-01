import os
import sys
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import datetime
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
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_mezzanine_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 2, 22, 13, 30, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into mezzanine loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.mezzanine.loans_fact_table following SCD type 2

    #### Actions
    <ol>
    <li>Extract from mifostenant-uganda.m_loan</li>
    <li>Check for existing loan instances in ubuntu.mezzanine.loans_fact_table</li>
    <li>Update existing instances in ubuntu.mezzanine.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.mezzanine.loans_fact_table</li>
    </ol>
    """

    def get_loans_marked_as_complete_in_warehouse():
        """
        Retrieves the most recent record of a loan from the warehouse and returns it if it exists
        :return: pd.DataFrame or None
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        results = warehouse_hook.get_pandas_df(
            sql="SELECT DISTINCT(loan_mifos_id) FROM mezzanine.loans_fact_table_summary_view "
                "WHERE loan_status = 600"
        )

        return results['loan_mifos_id'].tolist()


    def existing_loan_ids() -> list:
        """
        Retrieves the most recent record of a loan from the warehouse and returns it if it exists
        :return: pd.DataFrame or None
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        results = warehouse_hook.get_pandas_df(
            sql="SELECT loan_mifos_id FROM mezzanine.loans_fact_table_summary_view "
                "WHERE is_most_recent_record = True AND loan_status != 600"
                # A loan does not change after it has been marked at status 600 (closed)
        )

        return results['loan_mifos_id'].tolist()


    def existing_version(mifos_id: int):
        """
        Retrieves the most recent record of a loan from the warehouse and returns it if it exists
        :param mifos_id: the loan_id as it is in mifostenant-uganda.m_loan
        :return: pd.DataFrame or None
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        results = warehouse_hook.get_pandas_df(
            sql="SELECT * FROM mezzanine.loans_fact_table_summary_view WHERE loan_mifos_id={} ".format(
                mifos_id
            )
        )
        return results


    def add_audit_details(new_row: pd.Series, record_created_on_date: datetime.datetime) -> tuple:
        """
        adds tracking details to a new loan record
        for example record created on date
        NB A new row here is the most recent version of a loan and thus expired on is Null and is most recent version is True
        :param new_row: a series object without audit details
        :param record_created_on_date: when a record was created
        :return: a tuple containing details for a new loan row
        """
        return tuple(
            new_row.append(pd.Series([
                record_created_on_date, None, True],
                index=['record_created_on_date', 'record_expired_on_date', 'is_most_recent_record']
            )).tolist()
        )


    def extract_load_loans_fact_table(**context) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse
        :param context: dictionary of predefined values
        :return: None
        """
        el_loans_fact_table(mifos_tenant='uganda', warehouse_schema='mezzanine')

    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='mezzanine',
            filters=None
        )

    t2 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )

    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )

    t2 >> t1

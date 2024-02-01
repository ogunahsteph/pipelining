import os
import sys
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, el_loans_fact_table

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

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
        'ETL_bloom2_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2021, 9, 29, 18, 00, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into bloomlive loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1,
        template_searchpath=['~/airflow/dags/DataPipelines/sql']
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.bloomlive.loans_fact_table following SCD type 2
    DAG is set to run daily at 6pm.

    #### Actions
    <ol>
    <li>Extract from mifostenant-safaricom.m_loan</li>
    <li>Check for existing loan instances in ubuntu.bloomlive.loans_fact_table</li>
    <li>Update existing instances in ubuntu.bloomlive.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.bloomlive.loans_fact_table</li>
    </ol>
    """


    def extract_load_loans_fact_table(**context) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse
        :param context: dictionary of predefined values
        :return: None
        """
        # Establish connections to the two databases
        el_loans_fact_table(
            mifos_tenant='safaricom',
            warehouse_schema='bloomlive',
            warehouse_filters="bloom_version = '2'"
        )

        warehouse_hook.run(
            sql="""
                update bloomlive.loans_fact_table 
                set 
                    record_created_on_month = date_part('month', record_created_on_date),
                    record_created_on_year = date_part('year', record_created_on_date)
                where 
                    record_created_on_month is null 
                    or record_created_on_year is null
            """
        )


    def refresh_materialized_views(**context):
        warehouse_hook.run(
            sql="""refresh materialized view concurrently bloomlive.loans_fact_table_materialized_summary_view"""
        )
        warehouse_hook.run(
            sql="""refresh materialized view concurrently bloomlive.loan_scoring_strategy_view"""
        )


    def update_null_model_versions():
        airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
        # update null model versions
        loans = warehouse_hook.get_pandas_df(
            sql="""
                select surrogate_id, model_version, disbursed_on_date from bloomlive.loans_fact_table where model_version is null
            """
        )

        model_versions = airflow_hook.get_pandas_df(
            sql="""
                        select model_version, end_date from bloom_pipeline.limit_refresh_push_summary
                        order by end_date asc
                    """
        )

        def get_mv(x):
            # Find the latest end date in the model versions list that is earlier than the loan's disbursal date
            end_date = model_versions[model_versions['end_date'].dt.date < x['disbursed_on_date']]['end_date'].max()
            # If a model version exists for the found end date, return it
            if model_versions[model_versions['end_date'] == end_date]['model_version'].shape[0] > 0:
                return model_versions[model_versions['end_date'] == end_date]['model_version'].iloc[0]

        # Assign the latest model version to loans without a version
        if not loans.empty:
            loans.loc[loans['model_version'].isna(), 'model_version'] = loans[loans['model_version'].isna()].apply(
                lambda x: get_mv(x),
                axis=1
            )
            updates = [
                f"update bloomlive.loans_fact_table set model_version = '{r['model_version']}' where surrogate_id = {r['surrogate_id']}"
                for i, r in loans.iterrows()
            ]

            if len(updates) > 0:
                warehouse_hook.run(sql=updates)
        return True


    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='bloomlive',
            filters="and bloom_version = '2'"
        )
        refresh_materialized_views()


    t3 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )
    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )
    t4 = PythonOperator(
        task_id='refresh_materialized_views',
        provide_context=True,
        python_callable=refresh_materialized_views,
    )

    t5 = PythonOperator(
        task_id='update_null_model_versions',
        provide_context=True,
        python_callable=update_null_model_versions,
    )

    t3 >> t2 >> t4>>t5

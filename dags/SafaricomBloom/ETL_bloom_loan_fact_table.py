import os
import sys
import pendulum
import datetime
from airflow import DAG
from typing import Dict, Any
from datetime import timedelta
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, el_loans_fact_table
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

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
        'ETL_bloom_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2021, 9, 29, 18, 00, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into bloomlive loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### Summary
    This data pipeline extracts loan data from the MIFOSX core banking system and loads it into the data warehouse, specifically for loans belonging to Safaricom Bloom version 1.0.
    The data pipeline is triggered by the pipeline [ETL_bloom_dimensions](https://airflow.asantefsg.com/data-pipelines/dags/ETL_bloom_loans_fact_table/grid)
    
       #### Data:
       
       The data is located in the mifostenant-default.m_loan table and includes loans specific to Safaricom Bloom version 1.0.
       
       #### Tasks:
       
       1. extract_load_loans_fact_table:
    
          This PythonOperator task extracts loan data from MIFOSX and loads it into the warehouse. It defines a function that queries the source and target databases, updates the loans fact table, and inserts new loans with a Bloom version attribute.
    
       2. remove_warehouse_duplicates: 
       
          This PythonOperator task removes duplicates from the loans_fact_table in the warehouse. It executes a SQL query that identifies and deletes duplicate records based on loan attributes and record creation date.
       
       3. send_ms_teams_notification: 
       
          This MSTeamsWebhookOperator task sends a notification to a Microsoft Teams channel.
       
       #### Dependencies:
       The extract_load_loans_fact_table task must run before the remove_warehouse_duplicates task, and the remove_warehouse_duplicates task must run before the send_ms_teams_notification task.
       
       #### Functions:
       
       1. extract_load_loans_fact_table function: 
       
          This function extracts loans data from MIFOSX and loads it into the warehouse. It takes a dictionary of predefined values as input and returns None. The function establishes connections to the source and target databases, queries the source database to get the total number of loans, and then loops through the loans in batches. It uses SQL statements to query the target database for surrogate IDs for loan attributes, gets the loans data from MIFOSX, and updates the loans fact table in the target database.
    
       2. remove_warehouse_duplicates function:
          
          This function removes duplicate records from the loans_fact_table in the warehouse. It takes the Postgres connection ID and schema name as input via the **context parameter and returns nothing. The function executes an SQL query that identifies and deletes duplicate records based on loan attributes and record creation date. It uses a common table expression (CTE) named "rnked" that assigns a ranking value to each row based on the ascending order of the record_created_on_date column and deletes all but the oldest record for each set of duplicate rows.
                   
       Overall, these tasks and functions define a data pipeline that extracts loan data from the MIFOSX core banking system, loads it into the data warehouse, and removes duplicate records, specifically for Safaricom Bloom version 1.0 loans.
    """


    def extract_load_loans_fact_table(**context: Dict[str, Any]) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse.
        :param context: dictionary of predefined values
        :return: None
        """
        # Establish connections to the two databases
        el_loans_fact_table(
            mifos_tenant='default',
            warehouse_schema='bloomlive',
            warehouse_filters="bloom_version = 'bloom1_restructured'"
        )

    def remove_warehouse_duplicates(**context):
        """
        This function removes bloom 1.0 duplicates from the bloomlive.loans_fact_table table in the data warehouse.
        The duplicates are identified based on several loan attributes and only the oldest version of the duplicate rows is retained.

        :param **context: keyword arguments that specify the Postgres connection ID and the schema name.

        :return: None
        """
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='bloomlive',
            filters="and bloom_version = '1'"
        )

    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )
    t4 = PythonOperator(
        task_id='remove_warehouse_duplicates',
        provide_context=True,
        python_callable=remove_warehouse_duplicates,
    )
    t3 = MSTeamsWebhookOperator(
        task_id='send_ms_teams_notification',
        http_conn_id='msteams_webhook_url',
        message="""Bloom Pipeline""",
        subtitle="""Loans fact table has been refreshed""".strip(),
        dag=dag,
        retries=0
    )
    t2 >> t4 >> t3


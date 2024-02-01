import os
import sys
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
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
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'mpesa_statements_bloom_data_recon',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *',
        start_date=datetime.datetime(2023, 7, 12, 9, 45, tzinfo=local_tz),
        tags=['reconciliation'],
        description='reconcile mpesa transactions data into mifos',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Prepare data that is on Mpesa transactions that is missing on MIFOS for reposting

    #### 
    """

    def replace_special_characters(string: str) -> str:
        """
        removes special characters from a string
        param string: the text to be cleaned up
        """
        special_characters = ['½', '¿', '~', '�', 'ï', 'Â', 'Ã¯']

        for char in special_characters:
            string = string.replace(char, '')
        return string

    def share_missing_mpesa_repayments(**context):
        """
        A function that fetches repayments and disbursements data from two different tables in the database,
        and inserts the filtered and processed data into another table.

        Parameters:
            **context (dict): The context variable that is passed into the function. Contains all relevant
            information regarding the execution context of the function.

        Returns:
            None

        """

        # Fetch the total number of repayments from the database
        total_repayments = warehouse_hook.get_pandas_df(
            sql="""
                select count(*) as total
                from bloomlive.raw_mpesa_transactions rmt                          
                where is_repayment and transaction_status = 'Completed'
            """
        )['total'].iloc[0]

        # Process repayments and disbursements data in batches
        chunk_size = 2500

        for num in range(0, total_repayments, chunk_size):
            # Fetch a batch of repayments data from the database
            repayments = warehouse_hook.get_pandas_df(
                sql="""
                select
                    id as warehouse_key, lftmsv.receipt_number as loan_ref, initiation_time,
                    paid_in, other_party_info, 
                    TRIM(split_part(other_party_info, '-', 1))::int AS store_number, 
                    rmt.receipt_number, rmt.is_repayment, 
                    'mpesa_statements' as "src"
                from bloomlive.raw_mpesa_transactions rmt 
                inner join (
                    with rnked as (
                        select id_trxn_linkd, id_loan_cntrct, trxn_stts, rank() over (partition by id_trxn_linkd order by surrogate_id asc) rnk
                        from bloomlive.transactions_data_dump tdd where is_repayment 
                    ) select * from rnked where rnk = 1
                ) tdd on rmt.receipt_number = tdd.id_trxn_linkd
                inner join bloomlive.loans_fact_table_materialized_summary_view lftmsv on tdd.id_loan_cntrct = lftmsv.cntrct_id 
                where rmt.is_repayment and rmt.transaction_status = 'Completed' and tdd.trxn_stts = 'Completed'
                order by rmt.id desc offset %(start_index)s limit %(chunksize)s
                """,
                parameters={'start_index': num, 'chunksize': chunk_size}
            )

            if repayments.shape[0] > 0:
                # Fetch the list of existing repayments from the gateway table
                already_existing = gateway_hook.get_pandas_df(
                    sql="""
                        select warehouse_key from bloom_staging.transactions_data_dump where warehouse_key in %(surrogate_ids)s
                        and src = 'mpesa_statements'
                    """,
                    parameters={'surrogate_ids': tuple([int(x) for x in repayments['warehouse_key'].tolist()])}
                )

                # Drop the already existing repayments from the batch
                repayments.drop(
                    index=repayments[repayments['warehouse_key'].isin(already_existing['warehouse_key'].tolist())].index,
                    inplace=True)

            if repayments.shape[0] > 0:
                repayments['record_created_on_timestamp'] = datetime.datetime.now()
                repayments.drop_duplicates(subset=['receipt_number'], inplace=True)

                gateway_hook.insert_rows(
                    table='bloom_staging.transactions_data_dump',
                    target_fields=[
                        'warehouse_key', 'loan_ref', 'initiation_time',
                        'paid_in', 'other_party_info',
                        'store_number', 'receipt_number', 'is_repayment', 'record_created_on_timestamp', 'src'
                    ],
                    rows=repayments[[
                        'warehouse_key', 'loan_ref', 'initiation_time',
                        'paid_in', 'other_party_info', 'store_number',
                        'receipt_number', 'is_repayment', 'record_created_on_timestamp', 'src'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

    def share_missing_mpesa_disbursements(**context):
        """
        Extracts disbursements data from the warehouse, processes it and inserts it into the gateway.

        :param context: A dictionary containing contextual information about the DAG run.
        """

        # Retrieve the total number of disbursements from the warehouse
        total_disbursements = warehouse_hook.get_pandas_df(
            sql="""
                select count(*) as total from bloomlive.raw_mpesa_transactions where is_disbursement
                and transaction_status = 'Completed' and details like '%Disbursement%'
            """
        )['total'].iloc[0]

        # Define the chunk size for retrieving disbursements data in batches
        chunk_size = 2500

        # Retrieve disbursements data in batches (updated for recon from oct 15th 2023)
        for num in range(0, total_disbursements, chunk_size):
            disbursements = warehouse_hook.get_pandas_df(
                sql=f"""
                    select
                    id as surrogate_id,
                    receipt_number,
                    case
                        when substring(split_part(reason_type, ' ', 1), 1) = '1' then ((withdrawn * -1) * 100) / 99.52
                        when substring(split_part(reason_type, ' ', 1), 1) = '7' then ((withdrawn * -1) * 100) / 96.88
                        when substring(split_part(reason_type, ' ', 1), 1) = '21' then ((withdrawn * -1) * 100) / 90.88
                        else withdrawn
                    end as loan_principal,
                    other_party_info::varchar,
                    initiation_time::varchar,
                    substring(details from '\w+(?=\s*-\s*)') as store_number,
                    substring(split_part(reason_type, ' ', 1), 1) as loan_term
                from
                    bloomlive.raw_mpesa_transactions rmt
                where
                    is_disbursement
                    and transaction_status = 'Completed'
                    and details like '%Disbursement%'
                    and rmt.completion_time > '2023-10-15 00:00:00'
                order by
                    rmt.id desc offset {num} limit {chunk_size}
                """,
            )

            # Remove already existing disbursements from the retrieved data
            if disbursements.shape[0] > 0:
                already_existing = tuple([int(x) for x in gateway_hook.get_pandas_df(
                    sql="""
                        select warehouse_key from bloom_staging.transactions_data_dump where warehouse_key in %(surrogate_ids)s
                        and src = 'mpesa_statements'
                    """,
                    parameters={'surrogate_ids': tuple([int(x) for x in disbursements['surrogate_id'].tolist()]) if
                    disbursements.shape[0] > 1 else f"({disbursements['surrogate_id'].iloc[0]})"}
                )['warehouse_key'].tolist()])

                disbursements.drop(
                    index=disbursements[disbursements['surrogate_id'].isin(already_existing)].index,
                    inplace=True)
                if disbursements.shape[0] > 0:
                    # Add a new column for the record created timestamp.
                    disbursements['record_created_on_timestamp'] = datetime.datetime.now()

                    # Add a new column to indicate that this is a disbursement record.
                    disbursements['is_disbursement'] = 1

                    # Replace NaN values with None to avoid issues with database insertion.
                    disbursements.replace({np.NAN: None}, inplace=True)
                    disbursements['src'] = 'mpesa_statements'

                    # def get_loan_term(x):
                    # disbursements['loan_term'] = disbursements['loan_term'].apply(lambda x: int(x) if not pd.isnull(x) and not x.isalpha() else np.NAN)
                    disbursements = disbursements[~disbursements['loan_term'].str.isalpha()]
                    disbursements.dropna(subset=['store_number'], inplace=True)

                    # Insert the processed data into the database.
                    gateway_hook.insert_rows(
                        table='bloom_staging.transactions_data_dump',
                        target_fields=[
                            'warehouse_key', 'receipt_number', 'initiation_time',
                            'loan_principal', 'other_party_info',
                            'store_number', 'loan_ref', 'loan_date', 'loan_term', 'is_disbursement', 'src',
                            'record_created_on_timestamp'
                        ],
                        rows=disbursements[[
                            'surrogate_id', 'receipt_number', 'initiation_time',
                            'loan_principal', 'other_party_info',
                            'store_number', 'receipt_number', 'initiation_time', 'loan_term', 'is_disbursement',
                            'src', 'record_created_on_timestamp'
                        ]].itertuples(index=False, name=None),
                        commit_every=0
                    )


    def trigger_ETL_safaricom_recons_mifos_posting(**context):
        """
        Trigger posting of data to mifos
        """
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_safaricom_recons_mifos_posting', conf={})

    t1 = PythonOperator(
        task_id='share_missing_mpesa_disbursements',
        provide_context=True,
        python_callable=share_missing_mpesa_disbursements,
    )
    t2 = PythonOperator(
        task_id='share_missing_mpesa_repayments',
        provide_context=True,
        python_callable=share_missing_mpesa_repayments,
    )
    t3 = PythonOperator(
        task_id='trigger_ETL_safaricom_recons_mifos_posting',
        provide_context=True,
        python_callable=trigger_ETL_safaricom_recons_mifos_posting,
    )

    t1 >> t2 >> t3

import sys, os, datetime
from datetime import timedelta
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
import utils.mambu as mambu
import utils.warehouse as warehouse

MAMBU = mambu.MAMBU()
WAREHOUSE = warehouse.WAREHOUSE()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

SPath = os.path.dirname(os.path.realpath(__file__))
mapping_file = os.path.join(SPath, 'Mapper.xlsx')

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

with DAG(
        'ETL_MTN_Uganda_Dimensions',
        default_args=default_args,
        catchup=False,
        start_date=datetime.datetime(2023, 10, 31, 12, 50, tzinfo=local_tz),
        tags=['dimensions', 'mtn', 'uganda'],
        description='Extract dimensions data from CBS to DWH',
        user_defined_macros=default_args,
        max_active_runs=1,
        schedule_interval='0 * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
) as dag:
    # DOCS
    dag.doc_md = """
    ### MTN Uganda Dimensions ETL
    Extracts dimensions data from CBS and loads to the DWH
    """


    def extract_load_product_dimension(**context) -> None:
        """
        Extracts and loads MTN products to DWH
        """
        products_df = pd.DataFrame()

        offset = 0
        limit = 1000
        while True:
            data = MAMBU.get_loan_products(product="MTN", offset=offset, limit=limit)
            if len(data) == 0:
                break
            products_df = pd.concat([products_df, data])
            offset += limit

        # map data columns between the 2 sources
        mapping_data = pd.read_excel(mapping_file, sheet_name='products')
        products_df = MAMBU.data_columns_mapper(products_df, mapping_data)

        # only required columns for DWH
        product_columns = mapping_data['DWH'].tolist()
        products_df = products_df[product_columns]
        products_df.drop_duplicates(inplace=True)

        # insert new records
        WAREHOUSE.insert_new_records_to_dwh(
            data=products_df,
            warehouse_db='mtn_uganda.product_dimension',
            columns=product_columns
        )

        # update existing data if any
        WAREHOUSE.update_dwh_data(
            warehouse_db='mtn_uganda.product_dimension',
            mambu_data=products_df,
            data_columns=product_columns
        )

    def extract_load_partner_dimension(**context) -> None:
        """
        Extracts partner to DWH
        """
        partners_df = pd.DataFrame()

        offset = 0
        limit = 1000
        while True:
            data = MAMBU.get_partners(filter="MTN Uganda", offset=offset, limit=limit)
            if len(data) == 0:
                break
            partners_df = pd.concat([partners_df, data])
            offset += limit

        # map data columns between the 2 sources
        mapping_data = pd.read_excel(mapping_file, sheet_name='partners')
        partners_df = MAMBU.data_columns_mapper(partners_df, mapping_data)

        # only required columns for DWH
        partners_columns = mapping_data['DWH'].tolist()
        partners_df = partners_df[partners_columns]
        partners_df.drop_duplicates(inplace=True)

        # insert new records
        WAREHOUSE.insert_new_records_to_dwh(
            data=partners_df,
            warehouse_db='mtn_uganda.partner_dimension',
            columns=partners_columns
        )

        # update existing data if any
        WAREHOUSE.update_dwh_data(
            warehouse_db='mtn_uganda.partner_dimension',
            mambu_data=partners_df,
            data_columns=partners_columns
        )


    def extract_load_client_dimension(**context) -> None:
        """
        Extracts and loads MTN clients to DWH
        """
        clients_df = pd.DataFrame()

        offset = 0
        limit = 1000
        while True:
            data = MAMBU.get_clients(offset=offset, limit=limit)
            if len(data) == 0:
                break
            clients_df = pd.concat([clients_df, data])
            offset += limit

        # map data columns between the 2 sources
        mapping_data = pd.read_excel(mapping_file, sheet_name='clients')
        clients_df = MAMBU.data_columns_mapper(clients_df, mapping_data)

        # only required columns for DWH
        client_columns = mapping_data['DWH'].tolist()
        clients_df = clients_df[client_columns]
        clients_df.drop_duplicates(inplace=True)

        # load only for the current partner(center on Mambu)
        partners = WAREHOUSE.get_partners(table_name='mtn_uganda.partner_dimension', columns=['encoded_key'])
        clients_df = clients_df[clients_df['assigned_centre_key'].str.strip().isin(partners['encoded_key'].str.strip())]

        # insert new records
        WAREHOUSE.insert_new_records_to_dwh(
            data=clients_df,
            warehouse_db='mtn_uganda.client_dimension',
            columns=client_columns
        )

        # update existing data if any
        WAREHOUSE.update_dwh_data(
            warehouse_db='mtn_uganda.client_dimension',
            mambu_data=clients_df,
            data_columns=client_columns
        )
    def extract_load_loans_dimension(**context) -> None:
        """
        Extracts and loads MTN clients to DWH
        """
        loans_df = pd.DataFrame()

        offset = 0
        limit = 1000
        while True:
            data = MAMBU.get_loans(offset=offset, limit=limit)
            if len(data) == 0:
                break
            loans_df = pd.concat([loans_df, data])
            offset += limit


        # map data columns between the 2 sources
        mapping_data = pd.read_excel(mapping_file, sheet_name='loans')
        loans_df = MAMBU.data_columns_mapper(loans_df, mapping_data)

        # only required columns for DWH
        loan_columns = mapping_data['DWH'].tolist()
        loans_df = loans_df[loan_columns]
        loans_df.drop_duplicates(inplace=True)

        # load only for the current partner(center on Mambu)
        partners = WAREHOUSE.get_partners(table_name='mtn_uganda.partner_dimension', columns=['encoded_key'])
        loans_df = loans_df[loans_df['assigned_centre_key'].str.strip().isin(partners['encoded_key'].str.strip())]

        # insert new records
        WAREHOUSE.insert_new_records_to_dwh(
            data=loans_df,
            warehouse_db='mtn_uganda.loans_fact_table',
            columns=loan_columns
        )

        # update existing data if any
        WAREHOUSE.update_dwh_data(
            warehouse_db='mtn_uganda.loans_fact_table',
            mambu_data=loans_df,
            data_columns=loan_columns
        )

    def extract_load_transactions_dimension(**context) -> None:
        """
        Extracts and loads MTN clients to DWH
        """
        # load only for the current partner(center on Mambu)
        partners = WAREHOUSE.get_partners(table_name='mtn_uganda.partner_dimension', columns=['encoded_key'])

        transactions_df = pd.DataFrame()

        offset = 0
        limit = 1000
        while True:
            data = MAMBU.search_transactions(
                offset=offset,
                limit=limit,
                centre_keys=partners['encoded_key'].tolist()
            )

            if len(data) == 0:
                break
            transactions_df = pd.concat([transactions_df, data])
            offset += limit

        # map data columns between the 2 sources
        mapping_data = pd.read_excel(mapping_file, sheet_name='transactions')
        transactions_df = MAMBU.data_columns_mapper(transactions_df, mapping_data)

        # only required columns for DWH
        transaction_columns = mapping_data['DWH'].tolist()
        transactions_df = transactions_df[transaction_columns]
        transactions_df.drop_duplicates(inplace=True)

        # insert new records
        WAREHOUSE.insert_new_records_to_dwh(
            data=transactions_df,
            warehouse_db='mtn_uganda.transactions_dimension',
            columns=transaction_columns
        )

        # update existing data if any
        WAREHOUSE.update_dwh_data(
            warehouse_db='mtn_uganda.transactions_dimension',
            mambu_data=transactions_df,
            data_columns=transaction_columns
        )


    partner = PythonOperator(
        task_id='ETL_partner_dimension',
        provide_context=True,
        python_callable=extract_load_partner_dimension,
    )

    products = PythonOperator(
        task_id='ETL_products_dimension',
        provide_context=True,
        python_callable=extract_load_product_dimension,
    )

    clients = PythonOperator(
        task_id='ETL_clients_dimension',
        provide_context=True,
        python_callable=extract_load_client_dimension,
    )

    loans = PythonOperator(
        task_id='ETL_loans_dimension',
        provide_context=True,
        python_callable=extract_load_loans_dimension,
    )

    transactions = PythonOperator(
        task_id='ETL_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_teams',
        http_conn_id='msteams_webhook_url',
        message="""MTN Data Dimensions""",
        subtitle="""MTN Data Extracted from Mambu and loaded to DWH""".strip(),
        dag=dag,
        retries=0
    )

    partner >> products >> clients >> loans >> transactions >> notify_teams





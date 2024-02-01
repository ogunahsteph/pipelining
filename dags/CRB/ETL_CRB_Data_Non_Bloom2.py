import os
import sys
import logging
import pendulum
import numpy as np
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from concurrent.futures import ThreadPoolExecutor
import warnings

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

from dags.CRB.helper_methods import (
    exceptions_file_data,
    upload_exceptions_data,
    upload_crb_data,
    data_cleaning,
    process_data
)

warnings.filterwarnings("ignore")

log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_CRB_Data_Non_Bloom2',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 0 * * 1,3,5',
        start_date=datetime(2023, 10, 19, 2, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='CRB Data',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY

    """

    dag.log_level = 'ERROR'

    mapped_col_headers = ['Repayment Period', 'Gender', 'Instalment Due Date', 'Overdue Balance',
                          'Number of Days in Arrears', 'Number of Instalments in Arrears', 'Overdue Date',
                          'Prudential Risk Classification', 'Account Status', 'Account Status Date',
                          'Current Balance in Kenya Shillings', 'Instalment Amount',
                          'Date of Latest Payment', 'Last Payment Amount']


    def push_mobile_credit_data(loan, product, mifos_tenant: str):
        current_date = datetime.now().strftime('%Y%m%d')

        export_data = []

        with ThreadPoolExecutor(max_workers=20) as executor:
            processed_data = []

            # Submit tasks to the executor
            for index, loan_data in loan.iterrows():
                future = executor.submit(process_data, loan_data, product, mifos_tenant)
                processed_data.append(future)

            # Collect the results
            for data in processed_data:
                df_result = data.result()
                export_data.append(df_result)

        data_columns = loan.columns.tolist()
        data_columns.extend(mapped_col_headers)

        export_data = [inner_list[0] for inner_list in export_data]

        export_data = pd.DataFrame(export_data, columns=data_columns)

        export_data, amounts_exception, missing_data_dict = data_cleaning(export_data, product,
                                                                          mapped_col_headers)  # data cleaning method

        upload_crb_data(export_data, product)  # Export CRB upload Data

        # Export exceptions files
        for column, missing_data_df in missing_data_dict.items():
            if not missing_data_df.empty:
                file_name = f'CRB-{str(product).title()}-Exception-{str(column).title()}-{str(current_date)}.xlsx'
                upload_exceptions_data(missing_data_df, file_name)

        if not amounts_exception.empty:
            file_name = f'CRB-{str(product).title()}-Exception-Amounts-Less-100-{str(current_date)}.xlsx'
            upload_exceptions_data(amounts_exception, file_name)


    def get_client_info(product, client_ids):
        if product == 'pronto':
            sql_query = f"""
                        SELECT * FROM pronto.client_summary_view 
                        WHERE mifos_id IN {tuple(client_ids)}
                        """
        elif product == 'jubilee':
            sql_query = f"""
                        SELECT * FROM jubilee.client_summary_view
                        WHERE mifos_id IN {tuple(client_ids)}
                    """
        elif product == 'solv':
            sql_query = f"""
                SELECT * FROM solv_bat.client_summary_view
                WHERE mifos_id IN {tuple(client_ids)}
            """
        elif product == 'tanda':
            sql_query = f"""
                SELECT * FROM tanda.client_summary_view
                WHERE mifos_id  IN {tuple(client_ids)}
            """
        elif product == 'jumia':
            sql_query = f"""
                        SELECT * FROM jumia.client_summary_view
                        WHERE mifos_id IN {tuple(client_ids)}
                    """
        elif product == 'bloom1':
            sql_query = f"""
                SELECT * FROM bloomlive.client_materialized_summary_view
                WHERE bloom_version='1'
                AND mifos_id IN {tuple(client_ids)}
            """
        elif product == 'bloom1-restructured':
            sql_query = f"""
                    SELECT * FROM bloomlive.client_materialized_summary_view
                    WHERE bloom_version='bloom1_restructured'
                    AND mifos_id IN {tuple(client_ids)}
                """

        elif product == 'bloom2':
            sql_query = f"""
                SELECT * FROM bloomlive.client_materialized_summary_view
                WHERE bloom_version='2'
                AND mifos_id IN {tuple(client_ids)}
            """

        result = warehouse_hook.get_pandas_df(sql_query)
        return result


    def push_credit_data(product):
        global loan_result
        if product == "bloom1":
            loan_result = warehouse_hook.get_pandas_df(
                sql=f"""
                    SELECT * FROM bloomlive.loans_fact_table_materialized_summary_view 
                    WHERE bloom_version='1' AND loan_status=600 
                    AND total_outstanding IS NOT NULL 
                    AND loan_status IN (300, 600, 700)""",
            )
        elif product == "bloom1-restructured":
            loan_result = warehouse_hook.get_pandas_df("""
                                    SELECT * FROM bloomlive.loans_fact_table_materialized_summary_view 
                                    WHERE bloom_version='bloom1_restructured' 
                                    AND total_outstanding IS NOT null
                                    AND loan_status IN (300, 600, 700)
                                    AND disbursed_on_date = '2023-04-28'""",
                                                       )
        elif product == "bloom2":
            loan_result = warehouse_hook.get_pandas_df("""
                                    SELECT * FROM bloomlive.loans_fact_table_materialized_summary_view 
                                    WHERE bloom_version='2' AND safaricom_loan_balance IS NOT NULL 
                                    AND loan_status IN (300, 600, 700)
                                    AND disbursed_on_date >='2021-11-11'""",
                                                       )

        elif product in ["jumia", 'tanda', 'jubilee', 'solv']:
            if product == 'solv':
                product = 'solv_bat'  # renaming for DWH schema
            loan_result = warehouse_hook.get_pandas_df(f"""
                                    SELECT * FROM {product}.loans_fact_table_summary_view 
                                    WHERE total_outstanding IS NOT NULL 
                                    AND loan_status IN (300, 600, 700)"""
                                                       )

        elif product == 'pronto':
            loan_result = warehouse_hook.get_pandas_df(f"""
                                    SELECT * FROM {product}.loans_fact_table_summary_view 
                                    WHERE total_outstanding IS NOT NULL
                                    AND product_mifos_id IN (4, 5, 6, 9, 12, 13, 16)
                                    AND loan_status IN (300, 600, 700)"""
                                                       )

        return loan_result


    def process_result(**context):
        run_products = {'jumia': 'tanda', 'solv': 'tanda', 'tanda': 'tanda',
                        'bloom1-restructured': 'bloom1restructure', 'pronto': 'pronto'}

        products_dict = context['dag_run'].conf.get('products', run_products)
        products = list(products_dict.keys())

        for product in products:
            loan_info = push_credit_data(product)
            mifos_tenant = products_dict[product]

            if not loan_info.empty:
                try:
                    excluded_accounts = exceptions_file_data(product)
                    # (exclude rows matching the exclude_accounts)
                    loan_info = loan_info[~loan_info['loan_mifos_id'].isin(excluded_accounts)]
                except:
                    pass

                client_info = get_client_info(product, loan_info['client_mifos_id'])
                loan_info = loan_info[loan_info['client_mifos_id'].isin(client_info['mifos_id'])]
                client_info = client_info.rename(columns={'mifos_id': 'client_mifos_id'})
                loan_data = loan_info.merge(client_info, on='client_mifos_id')
                push_mobile_credit_data(loan_data, product, mifos_tenant)

            else:
                print("No Loan Records found")


    process_loans = PythonOperator(
        task_id='process_loans_for_CRB',
        dag=dag,
        provide_context=True,
        python_callable=process_result
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_teams',
        http_conn_id='msteams_webhook_url',
        message="""CRB Non-Bloom2 Pipeline Update""",
        subtitle="""Non Bloom2 CRB files generated and uploaded to sharepoint""".strip(),
        dag=dag,
        retries=0
    )

    process_loans >> notify_teams

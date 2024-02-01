import os
import sys
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from io import StringIO
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.common import on_failure
from utils.aws_api import get_objects, save_file_to_s3


warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='monitoring')
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
        'ETL_bloom_retail_field_metrics',
        default_args=default_args,
        catchup=False,
        schedule_interval='30 11 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2023, 6, 26, 9, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load retail field metrics data into bloomlive dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### DAG SUMMARY
    Extracts data from s3 bucket shared by the partner and loads the data into the warehouse
    DAG is set to run daily at 11:30 am.
    
    Pass "files_date" parameter to fetch files for a specific date. For example, to fetch files for date 2023-01-05 (YY-mm-dd)
    pass below configuration parameters.
    
    The parameters have to be in valid JSON. Use double quotes for keys and values
    ```
     {"files_date": "20230621"}
    ```
    
    If the no "files_date" parameter is passed, the pipeline defaults to fetching files for the current date.
    
    ### Details
    RFM partner loads files into the shared s3 bucket location daily at 11 am.
    The partner provide us with access key and token to be able to connect to the bucket. 
    
    To retrieve these files, the files are copied from the s3 bucket, loaded into the airflow server,
    uploaded to Asante s3 location s3://afsg-ds-prod-postgresql-dwh-archive/safaricom_bloom/retail_field_metrics/,
    loaded into the data warehouse
    and finally removed from the airlfow server in order to release space.

    """


    def copy_files_from_s3_bucket_to_airflow_server(**context):
        # Get date from dag run configuration, or use yesterday's date if not provided
        file_date = context['dag_run'].conf.get('files_date', str(datetime.datetime.today().date().strftime('%Y%m%d')))

        files = get_objects(
            aws_access_key_id=Variable.get('RFM_aws_access_key_id'),
            aws_secret_access_key=Variable.get('RFM_aws_secret_access_key'),
            search=file_date,
            bucket_name=Variable.get('RFM_aws_s3_bucket_name')
        )
        for file in files:
            s3_key = f"safaricom_bloom/retail_field_metrics/{file['name']}"
            csv_buffer = StringIO()
            file['data'].to_csv(csv_buffer, index=False)
            save_file_to_s3(
                s3_file_key=s3_key,
                bucket_name='afsg-ds-prod-postgresql-dwh-archive',
                file_bytes=csv_buffer.getvalue()
            )
            csv_buffer.seek(0)
            file['data']['Description'] = file['data']['Description'].apply(
                lambda x: str(x).replace('_x000d_', '').replace('\n', '') if not pd.isnull(x) else x)
            warehouse_hook.insert_rows(
                table='bloomlive.retail_field_metrics',
                target_fields=[
                    'outlet_id', 'question', 'question_sub_category_id', 'question_text', 'category', 'sub_category',
                    'sku_category', 'status', 'date_answered', 'expected_answer', 'actual_answer', 'outlet',
                    'display_name', 'contact', 'contact_phone', 'phone_number', 'latitude', 'longitude', 'outlet_type',
                    'description', 'market_name', 'is_scorable', 'geo_location', 'geo_radius_in_meters',
                    'geo_level_3_up',
                    'geo_level_2_up', 'geo_level_1_up', 'outlet_creation_date', 'team', 'channel', 'iso_date',
                    'distributor',
                    'merch_comment', 'shop_number', 'outlet_code', 'units_per_case', 'geo_level_4_up', 'geo_level_5_up',
                    'geo_level_6_up'
                ],
                replace=True,
                replace_index=['outlet_id', 'question', 'question_sub_category_id', 'category'],
                rows=tuple(file['data'][[
                    'Outlet ID', 'Question', 'Question Sub Category ID', 'Question Text', 'Category', 'Sub Category',
                    'Sku Category', 'Status', 'Date Answered', 'Expected Answer', 'Actual Answer', 'Outlet',
                    'Display Name', 'Contact', 'Contact Phone', 'Phone Number', 'Latitude', 'Longitude', 'Outlet Type',
                    'Description', 'Market Name', 'Is Scorable', 'Geo Location', 'Gps Radius In Meters',
                    'Geo Level 3 Up',
                    'Geo Level 2 Up', 'Geo Level 1 Up', 'Outlet Creation Date', 'Team', 'Channel', 'Iso Date',
                    'Distributor',
                    'Merch Comment', 'Shop Number', 'Outlet Code', 'Units Per Case', 'Geo Level 4 Up', 'Geo Level 5 Up',
                    'Geo Level 6 Up'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                commit_every=250
            )


    t1 = PythonOperator(
        task_id='copy_files_from_s3_bucket_to_warehouse',
        python_callable=copy_files_from_s3_bucket_to_airflow_server,
        retries=0
    )


import os
import sys
import logging
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.metabase_api import get_dataset
from utils.common import on_failure

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ELT_bloom_till_activity_raw_transactions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 4 * * *',
        start_date=datetime.datetime(2022, 6, 22, 18, 00, tzinfo=local_tz),
        tags=['extract_load', 'safaricom_bloom'],
        description='Extract Load Safaricom Bloom till activity raw data',
        user_defined_macros=default_args,
        max_active_runs=5
) as dag:
    # DOCS
    dag.doc_md = """
    #### DAG SUMMARY
    This data pipeline extracts data from [**Metabase**](https://data.asante.helaplus.com/) via API and stores it in the data warehouse table bloomlive.till_activity_dimension.
    
    The DAG accepts "data_date" parameter which specified the date for which till activity data will be fetched. For example, to fetch till activity data for date 2023-01-05 (YY-mm-dd)
    pass below configuration parameters.
    
    The parameters have to be in valid JSON. Use double quotes for keys and values
    ```
    {"data_date": "2023-01-05"}
    ```
    
    Read the full documentation [**here**](https://github.com/Asante-FSG/DataPipelines#readme)
    

    #### Actions
    <ol>
    <li>Extract data from Metabase API.</li>
    <li>Load data into the data warehouse.</li>
    </ol>
    """


    def get_till_activity_data(start_time: str, end_time: str) -> pd.DataFrame or None:
        return get_dataset(
            query=f"""
                select `payments`.`account_no`, `payments`.`amount`, `payments`.`balance_after`, `payments`.`client_name`,
                `payments`.`comments`, `payments`.`created_at`, `payments`.`id`, `payments`.`phone`, `payments`.`status`,
                `payments`.`store_number`, trim(`payments`.`transaction_id`) as transaction_id, `payments`.`transaction_time`, `payments`.`type`,
                `payments`.`updated_at` from `payments`
                where `payments`.`transaction_time` between '{start_time}' and '{end_time}'  
            """
        )

    def extract_load_payments(**context) -> None:
        """
        Retrieves data from Metabase and stores it into the warehouse
        """

        # Retrieve data_date from the DAG run configuration or use today's date
        data_date = context['dag_run'].conf.get('data_date',  datetime.datetime.strftime((datetime.datetime.today() - datetime.timedelta(days=1)).date(), '%Y-%m-%d'))

        if data_date is not None:
            dt = datetime.datetime.strptime(data_date, '%Y-%m-%d')
            date = f'{dt.year}-{dt.month if len(str(dt.month)) > 1 else "0" + str(dt.month)}-{dt.day if len(str(dt.day)) > 1 else "0" + str(dt.day)}'

            for hour in range(0, 24):  # Iterate over hours
                start_date = date + f' {hour if len(str(hour)) > 1 else "0" + str(hour)}:00:00'
                end_date = date + f' {hour if len(str(hour)) > 1 else "0" + str(hour)}:59:59'

                logging.warning(f'year: {dt.year}, month: {dt.month}, day: {dt.day}, hour: {hour}. Between {start_date} and {end_date}')

                # Retrieve payments data from Metabase
                payments = get_till_activity_data(start_time=start_date, end_time=end_date)
                payments['transaction_time'] = pd.to_datetime(payments['transaction_time'])

                if payments.shape[0] > 0:
                    # save_till_activity_data_to_s3(df=payments)
                    if dt >= datetime.datetime.today() + relativedelta(months=-3):
                        # Clean 'balance_after' column
                        payments['balance_after'] = payments['balance_after'].astype(str).str.replace(',', '').str.strip()

                        # Upsert payments data into the warehouse
                        warehouse_hook.insert_rows(
                            table='bloomlive.till_activity_dimension',
                            target_fields=[
                                'account_no', 'amount', 'balance_after', 'client_name', 'comments',
                                'created_at', 'foreign_id', 'phone', 'status', 'store_number',
                                'transaction_id', 'transaction_time', 'type', 'updated_at'
                            ],
                            rows=tuple(payments[[
                                'account_no', 'amount', 'balance_after', 'client_name', 'comments',
                                'created_at', 'id', 'phone', 'status', 'store_number', 'transaction_id',
                                'transaction_time', 'type', 'updated_at'
                            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                            replace=True,
                            replace_index=['transaction_id'],
                            commit_every=1000
                        )
                        logging.warning(f'{payments.shape[0]} records added')
                else:
                    logging.warning('No payments to upload!')


    t2 = PythonOperator(
        task_id='extract_load_payments',
        provide_context=True,
        python_callable=extract_load_payments,
    )
import os
import sys
import datetime
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
import pandas as pd

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
remita_hook = MySqlHook(mysql_conn_id='remita_server', schema='remita_staging')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")


with DAG(
        'EL_Remita_Customers_Data',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 9, 29, 4, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='extract load remita customers data',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    #### Remita Customers Data
    Extracts customers data from remita staging db to DWH every 3 hours."""

    def get_remita_data():
        remita_data = remita_hook.get_pandas_df(
            f"""SELECT * FROM remita_staging.Customers""").replace({None: pd.NA})
        warehouse_data = warehouse_hook.get_pandas_df(
            f"""SELECT * FROM remita.customer_dimension""").replace({None: pd.NA})
        remita_data.columns = remita_data.columns.str.lower()

        new_data = remita_data[~remita_data['id'].isin(warehouse_data['id'])]

        data_columns = warehouse_data.columns.tolist()

        if not new_data.empty:
            warehouse_hook.insert_rows(
                table='remita.customer_dimension',
                target_fields=data_columns,
                rows=new_data[data_columns].where(pd.notna(new_data), None).itertuples(index=False, name=None),
                commit_every=100
            )

        return True


    extract_load_data = PythonOperator(
        task_id='extract_load_customers_data',
        dag=dag,
        provide_context=True,
        python_callable=get_remita_data
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_on_teams',
        http_conn_id='msteams_webhook_url',
        message="""Remita Data""",
        subtitle="""Remita customers data successfully extracted and loaded to DWH.""".strip(),
        dag=dag,
        retries=0
    )

    extract_load_data >> notify_teams


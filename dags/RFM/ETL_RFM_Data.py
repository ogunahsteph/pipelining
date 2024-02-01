import os
import sys
import pytz
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
import json
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

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

with DAG(
        'ETL_RFM_Data',
        default_args=default_args,
        catchup=False,
        start_date=datetime.datetime(2023, 10, 31, 12, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract, Transform and Load data to DWH',
        user_defined_macros=default_args,
        max_active_runs=1,
        schedule_interval='15 12 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
) as dag:

    dag.doc_md = """
    ### Remita Data ETL 
    This DAG extracts RFM data bloomlive.retail_field_metrics, transforms the data, then loads new data to 
    retail_field_metrics.rfm_customers
    """

    def escape_quotes_on_sql(value):
        if isinstance(value, str):
            return value.replace("'", "''")
        return value


    def load_data(**context):
        question_columns = {
            84033: 'national_id',
            85473: 'store_number',
            84038: 'nominated_mobile_number',
            84039: 'primary_mobile_number',
            84040: 'alternative_mobile_number',
            84032: 'name_on_id',
            76988: 'nature_of_business',
            84041: 'gender',
            84046: 'record_of_tills',
            84043: 'no_of_tills',
            85472: 'has_signed_chattel_form'
        }
        raw_rfm_data = warehouse_hook.get_pandas_df(sql=f"""
                SELECT 
                    question,
                    actual_answer, 
                    outlet_id
                FROM bloomlive.retail_field_metrics
                """)

        raw_rfm_data_grouped = raw_rfm_data.groupby(['question', 'outlet_id'])

        grouped_data_dict = {}  # Initialize an empty dictionary

        for group, group_data in raw_rfm_data_grouped:
            outlet_id = group[1]  # Extract the 'outlet_id' from the group tuple
            question = group_data['question'].tolist()
            actual_answer = group_data['actual_answer'].tolist()

            # A dictionary entry for each 'outlet_id'
            if outlet_id not in grouped_data_dict:
                grouped_data_dict[outlet_id] = {
                    'question': [],
                    'actual_answer': [],
                }

            # Append the data to the dictionary entry
            grouped_data_dict[outlet_id]['question'].extend(question)
            grouped_data_dict[outlet_id]['actual_answer'].extend(actual_answer)

        data_frames = []

        for outlet_id, data_list in grouped_data_dict.items():
            df = pd.DataFrame(data_list)
            df = df.T
            df.columns = df.iloc[0]
            df.reset_index(drop=True, inplace=True)
            df.drop([0], inplace=True)
            df['outlet_id'] = outlet_id  # Add 'outlet_id' column
            data_frames.append(df)

        data_frames = [df.reset_index(drop=True) for df in data_frames]
        final_df = pd.concat(data_frames, ignore_index=True)
        columns = list(question_columns.keys())
        columns.append('outlet_id')
        final_df = final_df[columns]
        final_df.rename(columns=question_columns, inplace=True)
        df_to_check_updates = final_df

        existing_data = warehouse_hook.get_pandas_df(sql=f"""
                        SELECT 
                            outlet_id
                        FROM retail_field_metrics.rfm_customers
                        """)

        # drop records already existing in DWH
        final_df = final_df[~final_df['outlet_id'].isin(existing_data['outlet_id'])]

        if not final_df.empty:
            warehouse_hook.insert_rows(
                table='retail_field_metrics.rfm_customers',
                target_fields=final_df.columns.tolist(),
                rows=tuple(final_df[final_df.columns.tolist()].replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=50
            )

        columns_str = ', '.join(final_df.columns.tolist())  # to retrieve WH data
        existing_data = warehouse_hook.get_pandas_df(sql=f"""
                                SELECT 
                                    {columns_str}
                                FROM retail_field_metrics.rfm_customers
                                """)

        #updated DWH records
        merged_df = df_to_check_updates.merge(existing_data, how='outer', indicator=True)
        # Filtering out rows that differ in the new DataFrame
        new_or_diff_rows_df = merged_df[merged_df['_merge'] == 'left_only']
        updated_records = new_or_diff_rows_df.drop(columns=['_merge'])
        updated_records = updated_records.where(pd.notna(updated_records), None)

        updates = [
            f"UPDATE retail_field_metrics.rfm_customers SET " + ", ".join(
                f"{col} = NULL" if row[col] is None
                else f"{col} = '{escape_quotes_on_sql(row[col])}'" for col in final_df.columns.tolist() if col != 'outlet_id'
            ) + f" WHERE outlet_id = '{row['outlet_id']}'" for index, row in updated_records.iterrows()
        ]

        if len(updates) > 0:
            warehouse_hook.run(sql=updates)


    extract_load_data = PythonOperator(
        task_id='extract_transform_load_RFM_data',
        dag=dag,
        provide_context=True,
        python_callable=load_data
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_on_teams',
        http_conn_id='msteams_webhook_url',
        message="""RFM Data""",
        subtitle="""RFM data successfully extracted, transformed and loaded to DWH.""".strip(),
        dag=dag,
        retries=0
    )

    extract_load_data >> notify_teams

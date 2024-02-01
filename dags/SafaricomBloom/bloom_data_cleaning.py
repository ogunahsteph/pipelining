import os
import sys
import pytz
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utils.common import on_failure, cntrct_id_check_saf_disbursements_dump, saf_dumps_remove_trailing_zeros, store_national_id_updates


warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'Bloom_data_cleaning',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 12, 7, 22, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract column values and clean already settled data',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    clean data in the warehouse. Extract column values from raw data
    """

    def standardize_mobile_number(**context):
        """
        This function standardizes mobile numbers by removing invalid numbers and ensuring all Kenyan mobile numbers
        start with "254". It also removes the plus symbol in mobile numbers.

        param context: The context passed by Airflow.
        """
        # Remove mobile numbers that have letters in them
        warehouse_hook.run(
            sql="""
                UPDATE bloomlive.client_dimension
                SET mobile_number = null
                WHERE mobile_number SIMILAR TO '%[a-zA-Z]%'
            """
        )

        # Get clients with mobile numbers that start with '0' or '+'
        clients = warehouse_hook.get_pandas_df(
            sql="""
                 SELECT surrogate_id, mobile_number
                 FROM bloomlive.client_dimension
                 WHERE mobile_number LIKE '0%' OR mobile_number LIKE '+%'
             """
        )

        # Copy the mobile_number column to a new column
        clients['prev_mobile_number'] = clients['mobile_number']

        # Ensure all Kenyan mobile numbers start with 254
        clients['mobile_number'] = clients['mobile_number'].apply(
            lambda x: '254' + x[1:] if x[0] == '0' else x  # Trunk Prefix is Kenyan
        )

        # Remove plus (+) symbol in mobile numbers
        clients['mobile_number'] = clients['mobile_number'].apply(
            lambda x: x[1:] if x[0] == '+' else x  # Remove + letter
        )

        updates = []
        # UPDATE RECORDS
        for index, row in clients.replace({np.NAN: "null"}).iterrows():
            updates.append(
                f"UPDATE bloomlive.client_dimension SET mobile_number={row['mobile_number']} WHERE surrogate_id={row['surrogate_id']}  returning current_timestamp::varchar, '{row['mobile_number']}', '{row['prev_mobile_number']}', surrogate_id"
            )

        if len(updates) > 0:
            # Run the updates and store the results
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_mobile_number_updates(
                updated=updated,
                source='data cleaning',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )


    def clean_national_id_saf_client_activity_data_dumps(**context):
        """
        Cleans the national ID numbers in the SAF client activity data dump by removing records that have
        national ID numbers of '000000000' or '0', and removing the last two digits of national ID numbers
        that end with '.0'.

        Args:
            context: Context variable passed by Airflow.

        Returns:
            None
        """
        # Remove records with national ID numbers of '000000000' or '0'.
        warehouse_hook.run(sql="""
            update bloomlive.client_activity_data_dump set cust_id_nmbr = null where cust_id_nmbr = '000000000' or cust_id_nmbr = '0'
        """)

        # Remove the last two digits of national ID numbers that end with '.0'.
        warehouse_hook.run(
            sql="""
                update bloomlive.client_activity_data_dump set cust_id_nmbr = left(cust_id_nmbr, -2) WHERE cust_id_nmbr like '%.0'
            """
        )


    def clean_mobile_number_saf_client_activity_data_dumps(**context):
        """
        This function cleans the mobile numbers in the Safaricom client activity data dumps by removing the last two characters of
        mobile numbers that end in '.0' and setting mobile numbers that are '-1' to null.

        :param context: Airflow context variables
        """
        # Remove last two characters from mobile numbers ending with '.0'
        warehouse_hook.run(
            sql="""
                UPDATE bloomlive.client_activity_data_dump 
                SET mpsa_enty_phne = left(mpsa_enty_phne, -2) 
                WHERE mpsa_enty_phne LIKE '%.0'
            """
        )

        # Set mobile numbers that are '-1' to null
        warehouse_hook.run(
            sql="""
                UPDATE bloomlive.client_activity_data_dump 
                SET mpsa_enty_phne = null 
                WHERE mpsa_enty_phne = '-1'
            """
        )


    def clean_store_number_saf_client_activity_data_dumps(**context):
        """
        Cleans store numbers in the client activity data dump by removing the last two characters of any numbers
        that end with ".0" and updating the Bloomlive database using a warehouse hook.
        :param context: Task context passed by Airflow
        """
        warehouse_hook.run(
            sql="""
                update bloomlive.client_activity_data_dump set mpsa_orga_shrt = left(mpsa_orga_shrt, -2) 
                WHERE mpsa_orga_shrt like '%.0'
            """
        )


    def clean_store_number_bloom_client_dimension(**context):
        """
        This function cleans the store numbers in the Bloomlive client dimension table by replacing '-1' with null.

        :param context: dictionary of values representing the context of the current running instance of the DAG.
        """
        warehouse_hook.run(
            sql="""
                UPDATE bloomlive.client_dimension
                SET store_number = null
                WHERE store_number = '-1'
            """
        )


    def clean_store_number_saf_transactions_data_dumps(**context):
        """
        This function removes the trailing '.0' from the cd_mpsa_orga_shrt field in the transactions_data_dump table
        in the Bloomlive database schema. This is done using a warehouse hook to execute a SQL query.
        """
        warehouse_hook.run(
            sql="""
                update bloomlive.transactions_data_dump set cd_mpsa_orga_shrt = left(cd_mpsa_orga_shrt, -2) 
                WHERE cd_mpsa_orga_shrt like '%.0'
            """
        )


    def standardize_bloom_scoring_results_store_numbers(**context):
        """
        This function removes the trailing '.0' from the store_number field in the scoring_results table
        in the Bloomlive database schema. This is done using a warehouse hook to execute a SQL query.
        """
        warehouse_hook.run(
            sql="""
                update bloomlive.scoring_results set store_number = left(store_number, -2) 
                WHERE store_number like '%.0'
            """
        )


    def clean_national_ids_bloom(**context):
        """
        Cleans national ids in the bloomlive.client_dimension table:
        1. Removes records with invalid national id (000000000, 0, nan, None, -1, null).
        2. Removes national ids that are store numbers.
        3. Removes trailing zeros in national ids.
        4. Removes underscores in national ids.

        :param context: The Airflow context.
        """

        # Remove invalid national ids.
        warehouse_hook.run(sql="""
            update bloomlive.client_dimension set national_id = null where national_id = '000000000'
            or national_id in ('0', 'nan', 'None', '-1', 'null')
        """)

        # Remove national ids that are store numbers.
        warehouse_hook.run(
            sql="""
                update bloomlive.client_dimension set national_id = null 
                WHERE national_id = store_number and bloom_version = '2'
            """
        )

        # Remove trailing zeros in national ids.
        warehouse_hook.run(
            sql="""
                update bloomlive.client_dimension set national_id = left(national_id, -2) 
                WHERE national_id like '%.0'
            """
        )

        # Remove underscores in national ids.
        national_ids = warehouse_hook.get_pandas_df(
            sql="""
                select national_id, surrogate_id from bloomlive.client_summary_view
                where national_id like '%!_%' or national_id like '% _ %' 
                or national_id like '%_ %' or national_id like '%\_%'
            """
        )
        national_ids['prev_national_id'] = national_ids['national_id']
        national_ids['national_id'] = national_ids['national_id'].apply(
            lambda x: [y.strip() for y in x.split('_')][0]
        )

        updates = []
        # Update records.
        for index, row in national_ids.replace({np.NAN: "null"}).iterrows():
            national_id = str(row['national_id'])  # convert national_id to string
            if national_id.isdigit():  # check if national_id contains only digits
                updates.append(
                    f"UPDATE bloomlive.client_dimension SET national_id={national_id} WHERE surrogate_id={row['surrogate_id']}  returning current_timestamp::varchar, '{national_id}', '{row['prev_national_id']}', surrogate_id"
                )

        if len(updates) > 0:
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_national_id_updates(
                updated=updated,
                source='data cleaning',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )


    def clean_mobile_numbers_bloom(**context):
        """
        Cleans and standardizes mobile numbers in the Bloom database client_dimension table.

        Args:
            **context: Airflow context variable containing information about the DAG run.

        Returns:
            None

        """

        # Remove invalid mobile numbers - custom checks
        warehouse_hook.run(
            sql="""
                update bloomlive.client_dimension set mobile_number = null 
                WHERE mobile_number = '-1'
            """
        )

        # remove mobile numbers that are store numbers
        warehouse_hook.run(
            sql="""
                update bloomlive.client_dimension
                set mobile_number = null
                where mobile_number = store_number
            """
        )

        # Get mobile numbers with invalid characters
        mobile_numbers = warehouse_hook.get_pandas_df(
            sql="""
                select mobile_number, surrogate_id from bloomlive.client_summary_view
                where mobile_number like '%!_%' or mobile_number like '%.0' 
                or mobile_number like '% _ %' or mobile_number like '%_ %'
                or mobile_number like '%\_%'
            """
        )
        mobile_numbers['prev_mobile_number'] = mobile_numbers['mobile_number']

        # Remove invalid characters from mobile numbers
        mobile_numbers['mobile_number'] = mobile_numbers['mobile_number'].apply(
            lambda x: [y.strip() for y in x.split('_')][0].replace('.0', '')
        )

        updates = []
        # UPDATE RECORDS
        for index, row in mobile_numbers.replace({np.NAN: "null"}).iterrows():
            updates.append(
                f"UPDATE bloomlive.client_dimension SET mobile_number={row['mobile_number']} WHERE surrogate_id={row['surrogate_id']}  returning current_timestamp::varchar, '{row['mobile_number']}', '{row['prev_mobile_number']}', surrogate_id"
            )
        if len(updates) > 0:
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_mobile_number_updates(
                updated=updated,
                source='data cleaning',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )

        # remove invalid mobile numbers according to length and starting digit
        warehouse_hook.run(
            sql="""
                update bloomlive.client_dimension set mobile_number = null where 
                (length(trim(mobile_number)) != 12 and mobile_number like '254%') or
                (length(trim(mobile_number)) != 10 and mobile_number like '0%') or 
                (length(trim(mobile_number)) != 9 and mobile_number like '7%') or
                (length(trim(mobile_number)) != 9 and mobile_number like '1%')
            """
        )

    def remove_trailing_zeros_in_data_dumps(**context):
        saf_dumps_remove_trailing_zeros(
            warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        )

    def mark_incorrect_contract_ids_in_dumps(**context):
        cntrct_id_check_saf_disbursements_dump(warehouse_hook=warehouse_hook)


    def store_mobile_number_updates(updated: list, source: str, airflow_hook: MySqlHook):
        update_logs = pd.DataFrame(
            [result for results in updated for result in results],
            columns=['updated_on_timestamp', 'current_mobile_number', 'previous_mobile_number', 'client_surrogate_id']
        )
        update_logs['source'] = source

        utc_timezone = pytz.timezone('UTC')
        eat_timezone = pytz.timezone('Africa/Nairobi')

        update_logs['updated_on_timestamp'] = update_logs['updated_on_timestamp'].apply(
            lambda x: utc_timezone.localize(
                datetime.datetime.strptime(x.split("+")[0], '%Y-%m-%d %H:%M:%S.%f')).astimezone(eat_timezone)
        )
        airflow_hook.insert_rows(
            table='mobile_number_updates',
            target_fields=update_logs.reindex(sorted(update_logs.index)).columns.tolist(),
            rows=tuple(update_logs.reindex(sorted(update_logs.index)).replace({np.NAN: None}).itertuples(index=False,
                                                                                                         name=None)),
            commit_every=100
        )

    t1 = PythonOperator(
        task_id='standardize_mobile_number',
        provide_context=True,
        python_callable=standardize_mobile_number,
    )
    t2 = PythonOperator(
        task_id='clean_national_id_saf_client_activity_data_dumps',
        provide_context=True,
        python_callable=clean_national_id_saf_client_activity_data_dumps,
    )
    t3 = PythonOperator(
        task_id='clean_mobile_number_saf_client_activity_data_dumps',
        provide_context=True,
        python_callable=clean_mobile_number_saf_client_activity_data_dumps,
    )
    t4 = PythonOperator(
        task_id='clean_store_number_saf_client_activity_data_dumps',
        provide_context=True,
        python_callable=clean_store_number_saf_client_activity_data_dumps,
    )
    t5 = PythonOperator(
        task_id='clean_store_number_saf_transactions_data_dumps',
        provide_context=True,
        python_callable=clean_store_number_saf_transactions_data_dumps
    )
    t6 = PythonOperator(
        task_id='standardize_bloom_scoring_results_store_numbers',
        provide_context=True,
        python_callable=standardize_bloom_scoring_results_store_numbers,
    )
    t7 = PythonOperator(
        task_id='clean_national_ids_bloom',
        provide_context=True,
        python_callable=clean_national_ids_bloom
    )
    t8 = PythonOperator(
        task_id='clean_store_number_bloom_client_dimension',
        provide_context=True,
        python_callable=clean_store_number_bloom_client_dimension
    )
    t9 = PythonOperator(
        task_id='remove_trailing_zeros_in_data_dumps',
        python_callable=remove_trailing_zeros_in_data_dumps
    )
    t10 = PythonOperator(
        task_id='mark_incorrect_contract_ids_in_dumps',
        python_callable=mark_incorrect_contract_ids_in_dumps
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10

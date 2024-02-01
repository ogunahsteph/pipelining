import os
import sys
import datetime
import pendulum
import numpy as np
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
pronto_hook = MySqlHook(mysql_conn_id='pronto_db', schema='pronto_staging')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_pronto_scoring_results',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 8, 4, 10, 50, tzinfo=local_tz),
        tags=['Limit Refresh Push'],
        description='Load data into pronto scoring results tables',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts scoring results from the warehouse and adds them to pronto scoring results tables. 
    The pipeline is triggered manually with configuration parameters specifying which scoring
    results to post
    
    example:
    "conf": {
        "tasks_to_run": ["load_kenya_airways_scoring_results"]
    }
    """

    def load_kenya_airways_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.kenya_airways.scoring_results_kenya_airways_view and loads it into pronto_staging.scoring_results_kq
        """
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from kenya_airways.scoring_results_kenya_airways_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""delete from pronto_staging.scoring_results_kq where rec_id not in (126)"""
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select * from kenya_airways.scoring_results_kenya_airways_view
                        order by id desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                clients['created_at'] = clients['created_at'].apply(
                    lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f').strftime("%Y-%m-%d %H:%M:%S")
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results_kq',
                    target_fields=['id', 'tour_code', 'final_14_day_limit', 'created_at'],
                    rows=clients[[
                        'id', 'tour_code', 'final_14_day_limit', 'created_at'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def load_copia_device_financing_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.copia.scoring_results_smartphone_financing_view and loads it into pronto_staging.scoring_results_device_financing
        """
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from copia.scoring_results_smartphone_financing_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""delete from pronto_staging.scoring_results_device_financing where id not in (2946)"""
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select agent_id as customer_id, final_ksh_limit as final_allocated_limit from copia.scoring_results_smartphone_financing_view
                        order by created_at desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results_device_financing',
                    target_fields=['customer_id', 'final_allocated_limit'],
                    rows=clients[[
                        'customer_id', 'final_allocated_limit'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def load_rwanda_ac_group_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.ac_group_rwanda.scoring_results_ac_group_view and loads it into pronto_staging.scoring_results_ac_group
        """
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from ac_group_rwanda.scoring_results_ac_group_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""delete from pronto_staging.scoring_results_ac_group where id not in (47)"""
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select company_code as customer_id, final_allocated_30_day_limit, final_allocated_6_month_limit 
                        from ac_group_rwanda.scoring_results_ac_group_view
                        order by record_created_on_timestamp desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results_ac_group',
                    target_fields=['customer_id', 'final_allocated_30_day_limit', 'final_allocated_6_month_limit'],
                    rows=clients[[
                        'customer_id', 'final_allocated_30_day_limit', 'final_allocated_6_month_limit'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def load_bralirwa_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.stockpoint.scoring_results_srds_view and loads it into pronto_staging.scoring_results_bralirwa
        """
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from stockpoint.scoring_results_srds_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""delete from pronto_staging.scoring_results_bralirwa where id not in (17, 31)"""
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select vendors,	proxy_customer_id as customer_id, final_14_day_limit_usd,	
                        final_14_day_limit_rwf, final_14_day_limit_rwf as final_allocated_limit 
                        from stockpoint.scoring_results_srds_view
                        order by created_at desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results_bralirwa',
                    target_fields=['customer_id', 'vendors', 'final_14_day_limit_usd', 'final_14_day_limit_rwf', 'final_allocated_limit'],
                    rows=clients[[
                        'customer_id', 'vendors', 'final_14_day_limit_usd', 'final_14_day_limit_rwf', 'final_allocated_limit'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def load_hugeshare_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.hugeshare_eabl.scoring_results_hugeshare_pronto_view and loads it into pronto_staging.scoring_results
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from hugeshare_eabl.scoring_results_hugeshare_pronto_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""
                    delete from pronto_staging.scoring_results 
                    where customer_id not in (1743, 1744) and product_name = 'hugeshare'
                """
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select customer_code, final_3_day_limit, final_7_day_limit, final_14_day_limit,
                        'hugeshare' as product_name
                        from hugeshare_eabl.scoring_results_hugeshare_pronto_view
                        order by created_at desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results',
                    target_fields=['customer_code', '7_day_limit', '3_day_limit', '14_day_limit', 'product_name'],
                    rows=clients[[
                        'customer_code', 'final_7_day_limit', 'final_3_day_limit', 'final_14_day_limit', 'product_name'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def load_obradleys_scoring_results(**context) -> None:
        """
        Extracts data from ubuntu.obradleys_eabl.scoring_results_obradleys_pronto_view and loads it into pronto_staging.scoring_results
        """
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        def execute_task():
            total_clients = int(warehouse_hook.get_pandas_df(
                sql="""
                    select count(*) as ttl from obradleys_eabl.scoring_results_obradleys_pronto_view
                """
            ).iloc[0]['ttl'])

            chunk_size = 100
            pronto_hook.run(
                sql="""
                    delete from pronto_staging.scoring_results 
                    where customer_id not in (1743, 1744) and product_name = 'obradleys'
                """
            )

            for num in range(0, total_clients, chunk_size):
                clients = warehouse_hook.get_pandas_df(
                    sql="""
                        select customer_code, final_3_day_limit, final_7_day_limit, final_14_day_limit,
                        'obradleys' as product_name
                        from obradleys_eabl.scoring_results_obradleys_pronto_view
                        order by created_at desc offset %(start_index)s limit %(chunk_size)s
                    """,
                    parameters={'start_index': num, 'chunk_size': chunk_size}
                )

                pronto_hook.insert_rows(
                    table='pronto_staging.scoring_results',
                    target_fields=['customer_code', '7_day_limit', '3_day_limit', '14_day_limit', 'product_name'],
                    rows=clients[[
                        'customer_code', 'final_7_day_limit', 'final_3_day_limit', 'final_14_day_limit', 'product_name'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
                context['ti'].xcom_push(key='is_uploaded', value=True)
        else:  # no tasks should run
            context['ti'].xcom_push(key='is_uploaded', value=False)


    def compile_summary(**context):
        tasks = {
            'load_kenya_airways_scoring_results': 'Kenya Airways',
            'load_copia_device_financing_scoring_results': 'Copia',
            'load_rwanda_ac_group_scoring_results': 'AC Group Rwanda',
            'load_bralirwa_scoring_results': 'Bralirwa',
            'load_hugeshare_scoring_results': 'Hugeshare',
            'load_obradleys_scoring_results': 'Obradleys',
        }

        uploaded_results = []

        for task in tasks:
            if context['ti'].xcom_pull(task_ids=task, key='is_uploaded') is True:
                uploaded_results.append(tasks[task])

        uploaded_results = ', '.join(uploaded_results)

        # send MS Teams notification
        teams_notification = MSTeamsWebhookOperator(
            task_id="ms_teams_notify_failure",
            message="Pronto Scoring Results - PUSH",
            subtitle=f"""
                The following scoring results have been pushed to pronto live
                ; {uploaded_results}
            """.strip(),
            http_conn_id='msteams_webhook_url',
        )
        teams_notification.execute(context)

    # TASKS
    t1 = PythonOperator(
        task_id='load_kenya_airways_scoring_results',
        provide_context=True,
        python_callable=load_kenya_airways_scoring_results,
    )
    t2 = PythonOperator(
        task_id='load_copia_device_financing_scoring_results',
        provide_context=True,
        python_callable=load_copia_device_financing_scoring_results,
    )
    t3 = PythonOperator(
        task_id='load_rwanda_ac_group_scoring_results',
        provide_context=True,
        python_callable=load_rwanda_ac_group_scoring_results,
    )
    t4 = PythonOperator(
        task_id='load_bralirwa_scoring_results',
        provide_context=True,
        python_callable=load_bralirwa_scoring_results,
    )
    t5 = PythonOperator(
        task_id='load_hugeshare_scoring_results',
        provide_context=True,
        python_callable=load_hugeshare_scoring_results,
    )
    t6 = PythonOperator(
        task_id='load_obradleys_scoring_results',
        provide_context=True,
        python_callable=load_obradleys_scoring_results,
    )
    t7 = PythonOperator(
        task_id='compile_summary',
        provide_context=True,
        python_callable=compile_summary,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

import os
import sys
import datetime
import pendulum
import numpy as np
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure


mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-pronto')
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
smartcollect_hook = MySqlHook(mysql_conn_id='smartcollect', schema='smart_collect')



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
        'ETL_smartcollect',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2022, 8, 15, 8, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into smartcollect warehouse tables',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from smartcollect and adds non existent rows into various tables in warehouse
    DAG is set to run every two hours

    #### Actions
    <ol>
    <li>Extract from smartcollect.case_files load into ubuntu.smart_collect.case_file_dimension</li>
    <li>Extract from smartcollect.case_file_allocations load into ubuntu.smart_collect.case_file_allocation_dimension</li>
    <li>Extract from smartcollect.contact_statuses load into ubuntu.smart_collect.contact_status_dimension</li>
    <li>Extract from smartcollect.ptps load into ubuntu.smart_collect.promise_to_pay_dimension</li>
    <li>Extract from smartcollect.users load into ubuntu.smart_collect.users_dimension</li>
    <li>Extract from smartcollect.notes load into ubuntu.smart_collect.notes_fact_table</li>
    </ol>
    """


    def extract_load_case_file_dimension(**context) -> None:
        total_case_files = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.case_files cf
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_case_files, chunk_size):
            case_files = smartcollect_hook.get_pandas_df(
                sql="""
                    select id as smartcollect_id, external_id as loan_mifos_id, context as mifos_tenant 
                    from smart_collect.case_files cf order by id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.case_file_dimension
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(case_files['smartcollect_id'].tolist())}
            )
            case_files = case_files[~case_files['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if case_files.shape[0] > 0:
                warehouse_hook.insert_rows(
                    table='smart_collect.case_file_dimension',
                    target_fields=[
                        'smartcollect_id', 'loan_mifos_id', 'mifos_tenant'
                    ],
                    replace=False,
                    rows=tuple(case_files[[
                        'smartcollect_id', 'loan_mifos_id', 'mifos_tenant'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )

    def extract_load_users_dimension(**context) -> None:
        total_users = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.users u
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_users, chunk_size):
            users = smartcollect_hook.get_pandas_df(
                sql="""
                    select id as smartcollect_id, "names", email, phone 
                    from smart_collect.users u order by id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.users_dimension
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(users['smartcollect_id'].tolist())}
            )
            users = users[~users['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if users.shape[0] > 0:
                warehouse_hook.insert_rows(
                    table='smart_collect.users_dimension',
                    target_fields=[
                        'smartcollect_id', 'names', 'email', 'phone'
                    ],
                    replace=False,
                    rows=tuple(users[[
                        'smartcollect_id', 'names', 'email', 'phone'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )


    def extract_load_contact_status_dimension(**context) -> None:
        total_users = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.contact_statuses cs 
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_users, chunk_size):
            contact_statuses = smartcollect_hook.get_pandas_df(
                sql="""
                    select cs.id as smartcollect_id, cs.title, cs.description, ct.title as contact_type 
                    from smart_collect.contact_statuses cs left join smart_collect.contact_types ct 
                    on cs.contact_type_id = ct.id order by cs.id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.contact_status_dimension
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(contact_statuses['smartcollect_id'].tolist())}
            )
            contact_statuses = contact_statuses[~contact_statuses['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if contact_statuses.shape[0] > 0:
                warehouse_hook.insert_rows(
                    table='smart_collect.contact_status_dimension',
                    target_fields=[
                        'smartcollect_id', 'title', 'description', 'contact_type'
                    ],
                    replace=False,
                    rows=tuple(contact_statuses[[
                        'smartcollect_id', 'title', 'description', 'contact_type'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )

    def extract_load_ptp_dimension(**context) -> None:
        total_ptps = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.ptps p 
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_ptps, chunk_size):
            ptps = smartcollect_hook.get_pandas_df(
                sql="""
                    select id as smartcollect_id, case_file_id, amount, ptp_date as promise_to_pay_date,
                    amount_paid, date_paid, `type`, state, payment_plan_id, created_at, updated_at
                    from smart_collect.ptps p order by id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.promise_to_pay_dimension
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(ptps['smartcollect_id'].tolist())}
            )
            ptps = ptps[~ptps['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if ptps.shape[0] > 0:
                existing_case_files = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as case_file_key from smart_collect.case_file_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(ptps['case_file_id'].tolist())}
                )
                ptps = ptps.merge(
                    existing_case_files,
                    left_on='case_file_id',
                    right_on='case_file_key',
                    how='inner'
                )
                warehouse_hook.insert_rows(
                    table='smart_collect.promise_to_pay_dimension',
                    target_fields=[
                        'smartcollect_id', 'case_file_key', 'amount', 'promise_to_pay_date',
                        'amount_paid', 'date_paid', '_type', 'state', 'payment_plan_id', 'created_at',
                        'updated_at'
                    ],
                    replace=False,
                    rows=tuple(ptps[[
                        'smartcollect_id', 'case_file_key', 'amount', 'promise_to_pay_date',
                        'amount_paid', 'date_paid', 'type', 'state', 'payment_plan_id', 'created_at',
                        'updated_at'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )

    def extract_load_case_file_allocation_dimension(**context) -> None:
        total_allocations = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.case_file_allocations cfa 
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_allocations, chunk_size):
            allocations = smartcollect_hook.get_pandas_df(
                sql="""
                    select id as smartcollect_id, case_file_id as case_file_key, 
                    allocatable_id as allocatable_key, allocatable_type, date_from, date_to 
                    from smart_collect.case_file_allocations cfa order by id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.case_file_allocation_dimension
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(allocations['smartcollect_id'].tolist())}
            )
            allocations = allocations[~allocations['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if allocations.shape[0] > 0:
                existing_case_files = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as case_file_key from smart_collect.case_file_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(allocations['case_file_key'].tolist())}
                )
                existing_users = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as allocatable_key from smart_collect.users_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(allocations['allocatable_key'].tolist())}
                )

                allocations = allocations.merge(
                    existing_case_files,
                    left_on='case_file_key',
                    right_on='case_file_key',
                    how='inner'
                ).merge(
                    existing_users,
                    left_on='allocatable_key',
                    right_on='allocatable_key',
                    how='inner'
                )

                warehouse_hook.insert_rows(
                    table='smart_collect.case_file_allocation_dimension',
                    target_fields=[
                        'smartcollect_id', 'case_file_key', 'allocatable_key', 'allocatable_type',
                        'date_from', 'date_to'
                    ],
                    replace=False,
                    rows=tuple(allocations[[
                        'smartcollect_id', 'case_file_key', 'allocatable_key', 'allocatable_type',
                        'date_from', 'date_to'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )



    def extract_load_notes_fact_table(**context) -> None:
        total_notes = int(smartcollect_hook.get_pandas_df(
            sql=""" 
                select count(id) as ttl from smart_collect.notes n 
            """
        ).iloc[0]['ttl'])

        chunk_size = 1000

        for num in range(0, total_notes, chunk_size):
            notes = smartcollect_hook.get_pandas_df(
                sql="""
                    select id as smartcollect_id, contact_status_id as contact_status_key,
                    case_file_id as case_file_key, created_by as created_by_key,
                    update_type as entry_type, created_at as date_created
                    from smart_collect.notes n order by id desc
                    limit %(chunk_size)s offset %(start_index)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            existing = warehouse_hook.get_pandas_df(
                sql="""
                    select smartcollect_id from smart_collect.notes_fact_table
                    where smartcollect_id in %(ids)s
                """, parameters={'ids': tuple(notes['smartcollect_id'].tolist())}
            )
            notes = notes[~notes['smartcollect_id'].isin(existing['smartcollect_id'].tolist())]

            if notes.shape[0] > 0:
                existing_contact_statuses = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as contact_status_key from smart_collect.contact_status_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(notes['contact_status_key'].tolist())}
                )
                existing_case_files = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as case_file_key from smart_collect.case_file_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(notes['case_file_key'].tolist())}
                )
                existing_users = warehouse_hook.get_pandas_df(
                    sql="""
                        select smartcollect_id as created_by_key from smart_collect.users_dimension
                        where smartcollect_id in %(ids)s
                    """,
                    parameters={'ids': tuple(notes['created_by_key'].tolist())}
                )

                notes = notes.merge(
                    existing_case_files, left_on='case_file_key', right_on='case_file_key', how='inner'
                ).merge(
                    existing_users, left_on='created_by_key', right_on='created_by_key', how='inner'
                ).merge(
                    existing_contact_statuses, left_on='contact_status_key', right_on='contact_status_key', how='inner'
                )
                notes.drop_duplicates(subset=['smartcollect_id'], inplace=True)

                warehouse_hook.insert_rows(
                    table='smart_collect.notes_fact_table',
                    target_fields=[
                        'smartcollect_id', 'contact_status_key', 'case_file_key', 'created_by_key',
                        'entry_type', 'date_created'
                    ],
                    replace=False,
                    rows=tuple(notes[[
                        'smartcollect_id', 'contact_status_key', 'case_file_key', 'created_by_key',
                        'entry_type', 'date_created'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=100
                )

    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_case_file_dimension',
        provide_context=True,
        python_callable=extract_load_case_file_dimension,
    )
    t2 = PythonOperator(
        task_id='extract_load_users_dimension',
        provide_context=True,
        python_callable=extract_load_users_dimension,
    )
    t3 = PythonOperator(
        task_id='extract_load_contact_status_dimension',
        provide_context=True,
        python_callable=extract_load_contact_status_dimension,
    )
    t4 = PythonOperator(
        task_id='extract_load_ptp_dimension',
        provide_context=True,
        python_callable=extract_load_ptp_dimension,
    )
    t5 = PythonOperator(
        task_id='extract_load_case_file_allocation_dimension',
        provide_context=True,
        python_callable=extract_load_case_file_allocation_dimension
    )
    t6 = PythonOperator(
        task_id='extract_load_notes_fact_table',
        provide_context=True,
        python_callable=extract_load_notes_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6

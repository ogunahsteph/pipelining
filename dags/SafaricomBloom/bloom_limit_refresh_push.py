import os
import sys
import pytz
import logging
import numpy as np
import datetime
import pandas as pd
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from utils.common import on_failure, get_bloom_whitelist

gateway_server_ssh_hook = SSHHook(ssh_conn_id="ssh_gateway_server")
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
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
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'remote_files_directory_path': f'/root/data/safaricom_bloom/limit_refresh',
    'local_files_directory_path': f"/{os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))}/data/safaricom_bloom/limit_refresh"
}
local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'Bloom_limit_refresh_push',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 1, 11, 17, 30, tzinfo=local_tz),
        tags=['Limit Refresh Push'],
        description='bloom2 scoring results refresh push',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    This dag updates/inserts records into the safaricom whitelist via the gateway server
    #### Actions
    <ol>
    <li>updates store numbers whose limits have changed</li>
    <li>inserts new data if it exists</li>
    </ol>
    """


    def get_previous_whitelist() -> pd.DataFrame:
        return airflow_hook.get_pandas_df(
            sql="""
                select
                    Store_Number, Asante_Blacklist_Flag, Asante_Credit_Limit_1_Day, Asante_Credit_Limit_7_Day,
                    Asante_Credit_Limit_21_Day
                from bloom_pipeline.previous_whitelist
            """
        )


    def numeric_change_type(old_value, new_value):
        if float(old_value) > float(new_value):
            return 'negative'
        elif float(old_value) < float(new_value):
            return 'positive'
        elif float(old_value) == float(new_value):
            return


    def bool_change_type(old_value, new_value):
        if int(old_value) == 1 and int(new_value) == 0:
            return 'whitelist'
        elif int(old_value) == 0 and int(new_value) == 1:
            return 'blacklist'
        elif int(old_value) == int(new_value):
            return


    def get_limit_changes(row: pd.Series) -> dict:
        changes = {
            'blacklist_flag_change': bool_change_type(
                old_value=row['old_blacklist_flag'],
                new_value=row['new_blacklist_flag']
            ),
            '1_day_limit_change': numeric_change_type(
                old_value=row['old_final_1_limit'],
                new_value=row['new_final_1_limit']
            ),
            '7_day_limit_change': numeric_change_type(
                old_value=row['old_final_7_limit'],
                new_value=row['new_final_7_limit']
            ),
            '21_day_limit_change': numeric_change_type(
                old_value=row['old_final_21_limit'],
                new_value=row['new_final_21_limit']
            ),
            'has_positive_1_day_limit_change': 0,
            'has_positive_7_day_limit_change': 0,
            'has_positive_21_day_limit_change': 0,
            'has_negative_21_day_limit_change': 0,
            'has_negative_7_day_limit_change': 0,
            'has_negative_1_day_limit_change': 0,
            'has_1_day_limit_change': 0,
            'has_7_day_limit_change': 0,
            'has_21_day_limit_change': 0,
            'has_21_day_limit': 1 if row['new_final_21_limit'] > 0 else 0,
            'has_7_day_limit': 1 if row['new_final_7_limit'] > 0 else 0,
            'has_1_day_limit': 1 if row['new_final_1_limit'] > 0 else 0
        }


        for key in changes.keys():
            if changes[key] == 'positive' or changes[key] == 'negative':
                if key == '1_day_limit_change':
                    changes['has_1_day_limit_change'] = 1
                elif key == '7_day_limit_change':
                    changes['has_7_day_limit_change'] = 1
                elif key == '21_day_limit_change':
                    changes['has_21_day_limit_change'] = 1

                if changes[key] == 'positive':
                    if key == '1_day_limit_change':
                        changes['has_positive_1_day_limit_change'] = 1
                    elif key == '7_day_limit_change':
                        changes['has_positive_7_day_limit_change'] = 1
                    elif key == '21_day_limit_change':
                        changes['has_positive_21_day_limit_change'] = 1
                elif changes[key] == 'negative':
                    if key == '1_day_limit_change':
                        changes['has_negative_1_day_limit_change'] = 1
                    elif key == '7_day_limit_change':
                        changes['has_negative_7_day_limit_change'] = 1
                    elif key == '21_day_limit_change':
                        changes['has_negative_21_day_limit_change'] = 1
        return changes


    def create_and_store_inserts_sql_file(data, now, local_files_directory_path):
        # create SQL insert statements
        sql_statements = []
        for index, row in data.iterrows():
            new_data = f"({row['store_number']}, {row['blacklist_flag']}, {row['final_1_limit']}, {row['final_7_limit']}, {row['final_21_limit']}, '{str(now.strftime('%Y-%m-%d %H:%M:%S'))}', '{str(now.strftime('%Y-%m-%d %H:%M:%S'))}')"
            sql_statements.append(
                f"INSERT INTO dbo.PltAsanteFinanceList (Store_Number, Asante_Blacklist_Flag, Asante_Credit_Limit_1_Day, Asante_Credit_Limit_7_Day, Asante_Credit_Limit_21_Day, CreatedOn_Date, ModifiedOn_Date) VALUES {new_data};")

        # create output file
        inserts_file_timestamp = str(now).replace(' ', 'T').replace(':', '')
        inserts_file_name = f"inserts_{inserts_file_timestamp}.sql"
        inserts_file_path = f"{local_files_directory_path}/{inserts_file_name}"

        with open(inserts_file_path, 'w', encoding='utf-8') as sql_file:
            sql_file.write("\n".join(sql_statements))

        return inserts_file_name


    def get_data_to_be_inserted(**context):
        """
        retrieve the current limit refresh
        get new limit refresh data for those store numbers that are not already in the existing whitelist
        store new limits above in a file {{ local_files_directory_path }}inserts_{DATETIME}.csv
        save data in an xcom string to pass to a bash script later on
        """
        model_version = context['dag_run'].conf.get('model_version', None)
        now = datetime.datetime.now(tz=local_tz)

        data_to_be_inserted = airflow_hook.get_pandas_df(
            sql="""
                select
                    trim(leading '0' from store_number) as store_number, model_version, final_21_limit, final_7_limit, final_1_limit,blacklist_flag
                from bloom_pipeline.latest_limit_refresh llr
                where model_version = %(model_version)s
                and store_number not in (
                    select trim(leading '0' from store_number) from bloom_pipeline.previous_whitelist pw
                )
            """,
            parameters={
                'model_version': model_version
            }
        )

        data_to_be_inserted = data_to_be_inserted[~data_to_be_inserted['store_number'].str.contains('none', case=False)]

        if data_to_be_inserted.shape[0] > 0:
            # only execute when changing the shared whitelist

            inserts_file_name = create_and_store_inserts_sql_file(
                data_to_be_inserted,
                now,
                local_files_directory_path=context['templates_dict']['local_files_directory_path']
            )

            # push to DAG xcom
            context['ti'].xcom_push(key='inserts_file_name', value=inserts_file_name)

            data_to_be_inserted['has_negative_21_day_limit_change'] = 0
            data_to_be_inserted['has_negative_7_day_limit_change'] = 0
            data_to_be_inserted['has_negative_1_day_limit_change'] = 0
            data_to_be_inserted['has_21_day_limit_change'] = 0
            data_to_be_inserted['has_7_day_limit_change'] = 0
            data_to_be_inserted['has_1_day_limit_change'] = 0
            data_to_be_inserted['has_positive_21_day_limit_change'] = 0
            data_to_be_inserted['has_positive_7_day_limit_change'] = 0
            data_to_be_inserted['has_positive_1_day_limit_change'] = 0
            data_to_be_inserted['has_21_day_limit'] = data_to_be_inserted.apply(
                lambda x: 1 if x['final_21_limit'] > 0 else 0, axis=1)
            data_to_be_inserted['has_7_day_limit'] = data_to_be_inserted.apply(
                lambda x: 1 if x['final_7_limit'] > 0 else 0, axis=1)
            data_to_be_inserted['has_1_day_limit'] = data_to_be_inserted.apply(
                lambda x: 1 if x['final_1_limit'] > 0 else 0, axis=1)

            data_to_be_inserted.rename(columns={
                'final_21_limit': 'new_final_21_limit', 'final_7_limit': 'new_final_7_limit',
                'final_1_limit': 'new_final_1_limit', 'blacklist_flag': 'new_blacklist_flag'
            }, inplace=True)
            data_to_be_inserted['model_version'] = model_version
            data_to_be_inserted['is_insert'] = True

            # store inserts in database for reference
            airflow_hook.insert_rows(
                table='limit_refresh_change_tracker',
                target_fields=[
                    'store_number', 'model_version', 'new_final_21_limit', 'new_final_7_limit', 'new_final_1_limit',
                    'new_blacklist_flag',
                    'has_negative_21_day_limit_change', 'has_negative_7_day_limit_change',
                    'has_negative_1_day_limit_change', 'has_21_day_limit_change', 'has_7_day_limit_change',
                    'has_1_day_limit_change', 'has_positive_21_day_limit_change',
                    'has_positive_7_day_limit_change', 'has_positive_1_day_limit_change',
                    'has_21_day_limit', 'has_7_day_limit', 'has_1_day_limit', 'is_insert'
                ],
                replace=False,
                rows=tuple(data_to_be_inserted.replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=10000
            )
            context['ti'].xcom_push(key='total_inserts', value=f'{data_to_be_inserted.shape[0]} records inserted')
        else:
            context['ti'].xcom_push(key='total_inserts', value='0 records inserted')

        # free the memory
        del data_to_be_inserted


    def get_data_to_be_updated(**context):
        # !!! NEED TO PASS VARIABLE MODEL VERSION

        """
        retrieve the current limit refresh
        get new limit refresh data for those store numbers that are not already in the existing whitelist
        store new limits above in a file ~/data/Saf/limit_refresh/updates_{DATETIME}.csv
        save data in a xcom string to pass to a bash script later on
        """
        model_version = context['dag_run'].conf.get('model_version', None)
        now = datetime.datetime.now(tz=local_tz)

        prev_whitelist = airflow_hook.get_pandas_df(
            sql="""
                select
                    trim(Store_Number) as Store_Number, Asante_Blacklist_Flag as old_blacklist_flag,
                    Asante_Credit_Limit_1_Day as old_final_1_limit,
                    Asante_Credit_Limit_7_Day as old_final_7_limit, Asante_Credit_Limit_21_Day as old_final_21_limit
                from bloom_pipeline.previous_whitelist
            """
        ).drop_duplicates(subset='Store_Number')
        prev_whitelist['Store_Number'] = prev_whitelist['Store_Number'].apply(
            lambda x: str(x).replace('.0', '').strip() if not pd.isnull(x) else x
        )

        scoring_results = airflow_hook.get_pandas_df(
            sql="""
                select
                    store_number, model_version,
                    final_21_limit as new_final_21_limit, final_7_limit as new_final_7_limit,
                    final_1_limit as new_final_1_limit, blacklist_flag as new_blacklist_flag
                from bloom_pipeline.latest_limit_refresh llr
                where store_number in (
                    select trim(Store_Number) from bloom_pipeline.previous_whitelist pw
                    where Store_Number is not null
                )
            """
        )
        scoring_results['store_number'] = scoring_results['store_number'].apply(
            lambda x: str(x).replace('.0', '').strip() if not pd.isnull(x) else x
        )

        data = prev_whitelist.merge(scoring_results, left_on='Store_Number', right_on='store_number', how='right')
        data.drop(columns=['Store_Number'], inplace=True)

        if data.shape[0] > 0:
            # track changes
            for index, row in data.iterrows():
                changes = get_limit_changes(row)
                for change in changes:
                    data.loc[data.index[index], f'{change}'] = f'{changes[change]}'

            data_without_changes = data[~data.index.isin(data.dropna(
                subset=['1_day_limit_change', '7_day_limit_change', '21_day_limit_change', 'blacklist_flag_change'],
                how='all'
            ).index)]

            data.replace({'None': np.NAN}, inplace=True)

            data.dropna(
                subset=['1_day_limit_change', '7_day_limit_change', '21_day_limit_change', 'blacklist_flag_change'],
                how='all',
                inplace=True
            )

            # START create bash string ------------------------------------------------------------------

            changing_columns = {
                'Asante_Credit_Limit_1_Day': '1_day_limit_change', 'Asante_Credit_Limit_7_Day': '7_day_limit_change',
                'Asante_Credit_Limit_21_Day': '21_day_limit_change', 'Asante_Blacklist_Flag': 'blacklist_flag_change'
            }
            renamed_columns = {
                'Asante_Credit_Limit_21_Day': 'new_final_21_limit', 'Asante_Credit_Limit_7_Day': 'new_final_7_limit',
                'Asante_Credit_Limit_1_Day': 'new_final_1_limit', 'Asante_Blacklist_Flag': 'new_blacklist_flag'
            }
            local_files_directory_path = context['templates_dict']['local_files_directory_path']

            updates_file_timestamp = str(now).replace(' ', 'T').replace(':', '')
            updates_file_path = f"{local_files_directory_path}/updates_{updates_file_timestamp}.sql"

            with open(updates_file_path, 'w+') as updates_file:
                for index, row in data.iterrows():
                    store_number = row['store_number']
                    values = ""

                    for key in changing_columns.keys():
                        if not pd.isnull(row[changing_columns[key]]):
                            values += f'{key}={row[renamed_columns[key]]}, '

                    if len(values) > 0:
                        values = values[:-2]  # Remove the last comma and space
                        update_query = f"UPDATE partnerscores.dbo.PltAsanteFinanceList SET {values}, ModifiedOn_Date='{datetime.datetime.now()}' WHERE store_number = '{store_number}';\n"
                        updates_file.write(update_query)

            # push bash string to xcom
            context['ti'].xcom_push(
                key='updates_file_name',
                value=f'updates_{updates_file_timestamp}.sql'
            )

            # END create bash string -------------------------------------------------------------------------

            # save records with changes to db for reference
            data['is_update'] = True

            airflow_hook.insert_rows(
                table='limit_refresh_change_tracker',
                target_fields=[
                    'old_blacklist_flag', 'old_final_1_limit', 'old_final_7_limit', 'old_final_21_limit',
                    'store_number', 'model_version', 'new_final_21_limit', 'new_final_7_limit', 'new_final_1_limit',
                    'new_blacklist_flag', 'blacklist_flag_change',
                    '1_day_limit_change', '7_day_limit_change', '21_day_limit_change',
                    'has_positive_1_day_limit_change', 'has_positive_7_day_limit_change',
                    'has_positive_21_day_limit_change', 'has_negative_21_day_limit_change',
                    'has_negative_7_day_limit_change', 'has_negative_1_day_limit_change', 'has_1_day_limit_change',
                    'has_7_day_limit_change', 'has_21_day_limit_change',
                    'has_21_day_limit', 'has_7_day_limit',
                    'has_1_day_limit', 'is_update'
                ],
                replace=False,
                rows=tuple(data.replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=10000
            )
            if data_without_changes.shape[0] > 0:
                airflow_hook.insert_rows(
                    table='limit_refresh_change_tracker',
                    target_fields=[
                        'old_blacklist_flag', 'old_final_1_limit', 'old_final_7_limit', 'old_final_21_limit',
                        'store_number', 'model_version', 'new_final_21_limit', 'new_final_7_limit', 'new_final_1_limit',
                        'new_blacklist_flag',
                        'blacklist_flag_change', '1_day_limit_change', '7_day_limit_change',
                        '21_day_limit_change', 'has_positive_1_day_limit_change', 'has_positive_7_day_limit_change',
                        'has_positive_21_day_limit_change', 'has_negative_21_day_limit_change',
                        'has_negative_7_day_limit_change', 'has_negative_1_day_limit_change', 'has_1_day_limit_change',
                        'has_7_day_limit_change', 'has_21_day_limit_change',
                        'has_21_day_limit', 'has_7_day_limit',
                        'has_1_day_limit'
                    ],
                    replace=False,
                    rows=tuple(data_without_changes.replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=10000
                )
            context['ti'].xcom_push(key='total_updates', value=f'{data.shape[0]} records updated')

        else:
            context['ti'].xcom_push(key='total_updates', value='0 records updated')


        # free the memory
        del prev_whitelist
        del scoring_results
        del data


    def upload_previous_whitelist(**context):
        """
        store previous whitelist in a database table
        """
        past_two_limit_refreshes = warehouse_hook.get_records(
            sql="""
                select distinct(model_version) from bloomlive.scoring_results_view srv
                order by model_version desc limit 2
            """,
        )

        # only save the previous whitelist if a new model version is being used for the first time
        path = f"{context['templates_dict']['local_files_directory_path']}/prev_whitelist.csv"
        prev_whitelist = pd.read_csv(path)[1:]
        prev_whitelist.columns = [col.strip() for col in prev_whitelist.columns]
        prev_whitelist['Store_Number'] = prev_whitelist['Store_Number'].astype(str).apply(
            lambda x: x.strip().replace('.0', ''))

        prev_whitelist = prev_whitelist[[
            'Store_Number', 'Asante_Blacklist_Flag', 'Asante_Credit_Limit_1_Day', 'Asante_Credit_Limit_7_Day',
            'Asante_Credit_Limit_21_Day', 'CreatedOn_Date', 'ModifiedOn_Date'
        ]]
        logging.warning('inserting rows into previous whitelist table')

        airflow_hook.run(sql="""delete from bloom_pipeline.previous_whitelist""")
        data = get_bloom_whitelist(
            prev_whitelist.copy(),
            model_version=past_two_limit_refreshes[1][0],
            warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        )

        airflow_hook.insert_rows(
            table='bloom_pipeline.previous_whitelist',
            target_fields=[
                'bloom_version', 'Store_Number', 'mobile_number', 'primary_contact', 'first_name', 'middle_name',
                'last_name',
                'company_name', 'Asante_Blacklist_Flag', 'Asante_Credit_Limit_1_Day',
                'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day', 'Till_Suspended', 'limit_reason',
                'communication',
                'suspension_date', 'CreatedOn_Date', 'ModifiedOn_Date', 'secondary_contact', 'alternate_mobile_no1',
                'alternate_mobile_no2'
            ],
            replace=False,
            rows=tuple(data[~data['Store_Number'].str.contains('rows')][[
                'bloom_version', 'Store_Number', 'mobile_number', 'primary_contact', 'first_name', 'middle_name',
                'last_name',
                'company_name', 'Asante_Blacklist_Flag',
                'Asante_Credit_Limit_1_Day', 'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day',
                'is_suspended', 'proposed_summary_narration', 'communication_to_customer',
                'start_date', 'CreatedOn_Date', 'ModifiedOn_Date', 'secondary_contact', 'alternate_mobile_no1',
                'alternate_mobile_no2'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=10000
        )

        airflow_hook.run(sql="""delete from bloom_pipeline.latest_limit_refresh""")
        logging.warning('inserting rows into latest limit refresh table')
        airflow_hook.insert_rows(
            table='bloom_pipeline.latest_limit_refresh',
            target_fields=[
                'store_number', 'model_version', 'final_21_limit', 'final_7_limit', 'final_1_limit', 'blacklist_flag'
            ],
            rows=tuple(warehouse_hook.get_pandas_df(
                sql=f"""
                    select
                        store_number, model_version, final_21_limit, final_7_limit, final_1_limit, blacklist_flag
                    from bloomlive.scoring_results_view
                    where model_version = '{past_two_limit_refreshes[0][0]}' and store_number not like '0%' and store_number != '-1'
                """
            ).replace({np.NAN: None}).itertuples(index=False, name=None)),
            replace=False,
            commit_every=2500
        )


    def store_execution_summary(**context):
        model_version = context['dag_run'].conf.get('model_version', None)
        summary = pd.DataFrame({
            'model_version': model_version,
            'total_inserts': airflow_hook.get_records(
                sql="""
                    select count(*) from bloom_pipeline.limit_refresh_change_tracker
                    where model_version = %(model_version)s and is_insert = 1
                """, parameters={'model_version': model_version}
            )[0],
            'total_updates': airflow_hook.get_records(
                sql="""
                        select count(*) from bloom_pipeline.limit_refresh_change_tracker
                        where model_version = %(model_version)s and is_update = 1
                    """, parameters={'model_version': model_version}
            )[0],
            'start_date': context['dag_run'].start_date,
            'end_date': datetime.datetime.now(pytz.timezone('Africa/Nairobi'))
        }, index=[0])

        airflow_hook.insert_rows(
            table='limit_refresh_push_summary',
            target_fields=[
                'model_version', 'total_inserts', 'total_updates', 'start_date', 'end_date'
            ],
            replace=False,
            rows=tuple(summary.itertuples(index=False, name=None)),
            commit_every=1
        )


    def get_clients_to_zeroize(**context):
        clnts = airflow_hook.get_pandas_df(
            sql="""
                select Store_Number from bloom_pipeline.previous_whitelist pw 
                where Store_Number not in (select Store_Number from bloom_pipeline.latest_limit_refresh llr)
            """
        )
        chunk_size = 5000

        for num in range(0, clnts.shape[0], chunk_size):
            clnts2 = clnts[num:num + chunk_size]
            iterables = ', '.join("{0}".format(w) for w in clnts2['Store_Number'].tolist())

            # Execute zeroization for store numbers not in the current model version
            zeroize = SSHOperator(
                task_id='zeroize_client_limits',
                ssh_conn_id='ssh_gateway_server',
                command=f"""
                    /opt/mssql-tools/bin/sqlcmd -S %s,%s -U %s -P '%s' -d '%s' -Q "update dbo.PltAsanteFinanceList set Asante_Credit_Limit_1_Day = 0, Asante_Credit_Limit_7_Day = 0, Asante_Credit_Limit_21_Day = 0, ModifiedOn_Date = current_timestamp where Store_Number in ({iterables});"
                """ % (
                    Variable.get('safaricom_bloom_partner_scoring_results_database_host'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_port'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_username'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_password'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_name')
                ),
                retries=0,
                cmd_timeout=20
            )
            zeroize.execute(context)

        # add context
        context['ti'].xcom_push(key='total_zeroized', value=f'{clnts.shape[0]} records zeroized')

    def trigger_EL_live_bloom_whitelist(**context):
        """
        Trigger the EL live bloom whitelist DAG using the Airflow REST API.
        The data pipeline will fetch the latest limit whitelist from Safaricom end.
        :param context: The context dictionary used in Airflow to pass state between tasks.
        :return: None
        """
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='EL_live_bloom_whitelist', conf={})

    def copy_inserts_and_updates_files_from_airflow_server_to_gateway_server(**context):
        inserts_file_name = context['ti'].xcom_pull(task_ids='get_data_to_be_inserted', key='inserts_file_name')
        updates_file_name = context['ti'].xcom_pull(task_ids='get_data_to_be_updated', key='updates_file_name')

        local_filepath = [f"{context['templates_dict']['local_files_directory_path']}/" + inserts_file_name] if inserts_file_name is not None else []
        local_filepath += [f"{context['templates_dict']['local_files_directory_path']}/" + updates_file_name] if updates_file_name is not None else []

        remote_filepath = [f"{context['templates_dict']['remote_files_directory_path']}/" + inserts_file_name] if inserts_file_name is not None else []
        remote_filepath += [f"{context['templates_dict']['remote_files_directory_path']}/" + updates_file_name] if updates_file_name is not None else []

        operator = SFTPOperator(
            task_id="copy_inserts_and_updates_files",
            ssh_conn_id="ssh_gateway_server",
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation="put",
            create_intermediate_dirs=True,
            dag=dag,
            retries=0
        )
        operator.execute(context)

    def execute_inserts(**context):
        inserts_file_name = context['ti'].xcom_pull(task_ids='get_data_to_be_inserted', key='inserts_file_name')
        if inserts_file_name is not None:
            remote_files_directory_path = context['templates_dict']['remote_files_directory_path']
            SSHOperator(
                task_id='execute_inserts_',
                ssh_conn_id='ssh_gateway_server',
                command=f"""
                    /opt/mssql-tools/bin/sqlcmd -S %s,%s -U %s -P '%s' -d '%s' -i {remote_files_directory_path}/{inserts_file_name};
                """ % (
                    Variable.get('safaricom_bloom_partner_scoring_results_database_host'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_port'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_username'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_password'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_name')
                ),
                retries=0,
                cmd_timeout=300,
                do_xcom_push=False
            ).execute(context)
        else:
            logging.warning("There are 0 inserts to make")

    def execute_updates(**context):
        updates_file_name = context['ti'].xcom_pull(task_ids='get_data_to_be_updated', key='updates_file_name')
        if updates_file_name is not None:
            remote_files_directory_path = context['templates_dict']['remote_files_directory_path']
            SSHOperator(
                task_id='execute_updates_',
                ssh_conn_id='ssh_gateway_server',
                command=f"""
                    /opt/mssql-tools/bin/sqlcmd -S %s,%s -U %s -P '%s' -d '%s' -i {remote_files_directory_path}/{updates_file_name};
                """ % (
                    Variable.get('safaricom_bloom_partner_scoring_results_database_host'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_port'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_username'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_password'),
                    Variable.get('safaricom_bloom_partner_scoring_results_database_name')
                ),
                retries=0,
                cmd_timeout=300,
                do_xcom_push=False
            ).execute(context)
        else:
            logging.warning("There are 0 updates to make")

    common_params = {
        'remote_files_directory_path': default_args['remote_files_directory_path'],
        'local_files_directory_path': default_args['local_files_directory_path']
    }

    t1 = SSHOperator(
        task_id='get_previous_whitelist',
        ssh_conn_id='ssh_gateway_server',
        command="""
            /opt/mssql-tools/bin/sqlcmd -S %s,%s -U %s -P '%s' -d '%s' -Q "SELECT * FROM dbo.PltAsanteFinanceList;" -o {{ remote_files_directory_path }}/prev_whitelist.csv -s"," -w 700
        """ % (
            Variable.get('safaricom_bloom_partner_scoring_results_database_host'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_port'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_username'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_password'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_name')
        ),
        retries=0,
        cmd_timeout=300
    )
    t2 = SFTPOperator(
        task_id="copy_previous_whitelist_from_gateway_server_to_airflow_server",
        ssh_conn_id="ssh_gateway_server",
        local_filepath="{{ local_files_directory_path }}/prev_whitelist.csv",
        remote_filepath="{{ remote_files_directory_path }}/prev_whitelist.csv",
        operation="get",
        create_intermediate_dirs=True,
        dag=dag,
        retries=0,
    )
    t3 = PythonOperator(
        task_id='upload_previous_whitelist',
        python_callable=upload_previous_whitelist,
        templates_dict=common_params,
        retries=1
    )
    t4 = PythonOperator(
        task_id='get_data_to_be_inserted',
        provide_context=True,
        python_callable=get_data_to_be_inserted,
        templates_dict=common_params,
    )
    t5 = PythonOperator(
        task_id='get_data_to_be_updated',
        provide_context=True,
        python_callable=get_data_to_be_updated,
        templates_dict=common_params,
    )
    t6 = PythonOperator(
        task_id="copy_inserts_and_updates_files_from_airflow_server_to_gateway_server",
        provide_context=True,
        python_callable=copy_inserts_and_updates_files_from_airflow_server_to_gateway_server,
        templates_dict=common_params
    )
    t7 = PythonOperator(
        task_id='execute_inserts',
        provide_context=True,
        python_callable=execute_inserts,
        templates_dict=common_params
    )
    t8 = PythonOperator(
        task_id='execute_updates',
        provide_context=True,
        python_callable=execute_updates,
        templates_dict=common_params
    )
    t9 = PythonOperator(
        task_id='get_clients_to_zeroize',
        provide_context=True,
        python_callable=get_clients_to_zeroize
    )
    t10 = MSTeamsWebhookOperator(
        task_id='send_limit_refresh_push_summary',
        http_conn_id='msteams_webhook_url',
        message="""Bloom Limit Refresh - Push""",
        subtitle="""
            Limits pushed for model version {{dag_run.conf["model_version"]}}.
            {{ti.xcom_pull(task_ids='get_data_to_be_inserted', key='total_inserts')}}.
            {{ti.xcom_pull(task_ids='get_data_to_be_updated', key='total_updates')}}.
            {{ti.xcom_pull(task_ids='get_clients_to_zeroize', key='total_zeroized')}}.
        """.strip(),
        dag=dag,
        retries=0
    )
    t11 = PythonOperator(
        task_id='store_execution_summary',
        provide_context=True,
        python_callable=store_execution_summary
    )
    t12 = PythonOperator(
        task_id='trigger_EL_live_bloom_whitelist',
        provide_context=True,
        python_callable=trigger_EL_live_bloom_whitelist
    )
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12

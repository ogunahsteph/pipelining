import os
import sys
import datetime
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import on_failure, trigger_dag_remotely

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'solv_office_id': 7,
    'bloom1_office_id': 1,
    'bloom2_office_id': 1,
    'airtel_uganda_office_id': 1
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'warehouse_data_monitoring',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *', # every two hours
        start_date=datetime.datetime(2023, 4, 17, 8, 00, tzinfo=local_tz),
        tags=['data_monitoring'],
        description='Get Bloom Data Comparison Files',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
        ### DAG SUMMARY
        DAG is scheduled to run every 3 hours. Data is fetched from currency exchange API and added to the data warehouse
        on schema currency_exchange_rates
    """

    def get_transaction_summaries(**context):
        sources = [
            {'schema': 'bloomlive', 'filters': "and bloom_version = '2'", 'mifos-tenant': 'safaricom', 'data_pipeline': 'ETL_bloom2_dimensions'},
            {'schema': 'bloomlive', 'filters': "and bloom_version = '1'", 'mifos-tenant': 'default', 'data_pipeline': 'ETL_bloom_dimensions'},
            {'schema': 'bloomlive', 'filters': "and bloom_version = 'bloom1_restructured'", 'mifos-tenant': 'bloom1restructure', 'data_pipeline': 'ETL_bloom1_restructured_dimensions'},
            {'schema': 'solv_bat', 'filters': None, 'mifos-tenant': 'tanda', 'data_pipeline': 'ETL_Solv_dimensions'},
            {'schema': 'tanda', 'filters': None, 'mifos-tenant': 'tanda', 'data_pipeline': 'ETL_Tanda_dimensions'},
            {'schema': 'twiga', 'filters': None, 'mifos-tenant': 'tanda', 'data_pipeline': 'ETL_Twiga_dimensions'},
            {'schema': 'airtel_uganda_device_financing', 'filters': None, 'mifos-tenant': 'devices', 'data_pipeline': 'ETL_Airtel_UG_dimensions'},
            {'schema': 'jumia', 'filters': None, 'mifos-tenant': 'tanda', 'data_pipeline': 'ETL_Jumia_dimensions'},
            {'schema': 'pronto', 'filters': None, 'mifos-tenant': 'pronto', 'data_pipeline': 'ETL_pronto_dimensions'},
            {'schema': 'jubilee', 'filters': None, 'mifos-tenant': 'jubilee', 'data_pipeline': 'ETL_Jubilee_dimensions'},
        ]

        for src in sources:
            warehouse_transactions = warehouse_hook.get_pandas_df(
                sql=f"""
                    SELECT
                      date_part('year', transaction_date) as "year",
                        date_part('month', transaction_date) as "month",
                        count(*) as total_count,
                        sum(amount) as total_sum,
                        coalesce(sum(penalty_charges_portion_derived), 0) as sum_penalty_charges_portion_derived
                    FROM
                      {src['schema']}.transactions_dimension
                      where transaction_type_enum in (1, 2) {src['filters'] if src['filters'] is not None else ''}
                    group by date_part('year', transaction_date), date_part('month', transaction_date)
                    order by date_part('year', transaction_date) desc, date_part('month', transaction_date) desc
                """
            )
            sql = f"""
                SELECT mifos_id as product_mifos_id
                FROM {src['schema']}.product_dimension
            """
            sql = sql + f"where {src['filters'].replace('and', '')}" if src['filters'] is not None and 'bloom_version' in src['filters'] else sql

            product_ids = [x[0] for x in warehouse_hook.get_records(sql=sql)]
            mifos_transactions = mifos_hook.get_pandas_df(
                sql=f"""
                    select
                        year(mlt.transaction_date) as 'year',
                        month(mlt.transaction_date) as 'month',
                        count(*) as total_count,
                        sum(mlt.amount) as total_sum,
                        coalesce(sum(penalty_charges_portion_derived), 0) as sum_penalty_charges_portion_derived
                    from `mifostenant-{src['mifos-tenant']}`.m_loan ml
                    inner join `mifostenant-{src['mifos-tenant']}`.m_loan_transaction mlt on mlt.loan_id = ml.id
                    {'inner' if src['schema'] == 'bloomlive' else 'left'} join `mifostenant-{src['mifos-tenant']}`.m_payment_detail mpd on mpd.id = mlt.payment_detail_id
                    left join `mifostenant-{src['mifos-tenant']}`.m_payment_type mpt on mpt.id = mpd.payment_type_id
                    where mlt.transaction_type_enum in (1, 2) and mlt.is_reversed = 0
                    and ml.product_id in ({', '.join("{0}".format(w) for w in product_ids)})
                    group by year(mlt.transaction_date), month(mlt.transaction_date)
                    order by year(mlt.transaction_date) desc, month(mlt.transaction_date) desc
                """
            )

            merged = pd.merge(
                mifos_transactions,
                warehouse_transactions,
                on=["year", "month"],
                suffixes=("_mifos", "_warehouse")
            )

            merged['diff_count'] = merged['total_count_mifos'] - merged['total_count_warehouse']
            merged['diff_sum'] = merged['total_sum_mifos'] - merged['total_sum_warehouse']
            merged['diff_sum_penalty_charges_portion_derived'] = merged['sum_penalty_charges_portion_derived_mifos'] - merged['sum_penalty_charges_portion_derived_warehouse']
            merged['updated_on_timestamp'] = datetime.datetime.utcnow()

            for i, r in merged[
                (
                    (merged['diff_count'] != 0) |
                    (merged['diff_sum'] != 0) |
                    (merged['diff_sum_penalty_charges_portion_derived'] != 0)
                ) &
                (
                    (merged['year'] < datetime.datetime.today().year) |
                    ((merged['year'] == datetime.datetime.today().year) & (merged['month'] < datetime.datetime.today().month))
                )
            ].iterrows():
                trigger_dag_remotely(dag_id=f"{src['data_pipeline']}", conf={"year": r['year'], "month": r['month']})

            if src['filters'] is not None and 'bloom_version' in src['filters']:
                src['schema'] = f"""{src['schema']}{src['filters'].split(' ')[-1].replace("'", "")}""".strip()
            merged['warehouse_schema'] = src['schema']

            inserts = []
            for i, r in merged.iterrows():
                inserts.append(f"""
                    INSERT INTO monitoring.mifos_vs_warehouse_transactions (
                        year, month, total_count_mifos, diff_count, total_count_warehouse,
                        total_sum_mifos, diff_sum, total_sum_warehouse, updated_on_timestamp,
                        warehouse_schema, diff_sum_penalty_charges_portion_derived,
                        sum_penalty_charges_portion_derived_mifos, sum_penalty_charges_portion_derived_warehouse
                    ) VALUES (
                        {r['year']}, {r['month']}, {r['total_count_mifos']}, {r['diff_count']}, {r['total_count_warehouse']},
                        {r['total_sum_mifos']}, {r['diff_sum']}, {r['total_sum_warehouse']}, '{r['updated_on_timestamp']}',
                        '{r['warehouse_schema']}', {r['diff_sum_penalty_charges_portion_derived']},
                        {r['sum_penalty_charges_portion_derived_mifos']}, {r['sum_penalty_charges_portion_derived_warehouse']}
                    ) ON DUPLICATE KEY UPDATE
                        total_count_mifos = VALUES(total_count_mifos),
                        diff_count = VALUES(diff_count),
                        total_count_warehouse = VALUES(total_count_warehouse),
                        total_sum_mifos = VALUES(total_sum_mifos),
                        diff_sum = VALUES(diff_sum),
                        diff_sum_penalty_charges_portion_derived = VALUES(diff_sum_penalty_charges_portion_derived),
                        sum_penalty_charges_portion_derived_mifos = VALUES(sum_penalty_charges_portion_derived_mifos),
                        sum_penalty_charges_portion_derived_warehouse = VALUES(sum_penalty_charges_portion_derived_warehouse),
                        total_sum_warehouse = VALUES(total_sum_warehouse),
                        updated_on_timestamp = VALUES(updated_on_timestamp);
                """.strip())

            if len(inserts) > 0:
                airflow_hook.run(sql=inserts)

    def get_client_summaries(**context):
        sources = [
            {'schema': 'bloomlive', 'mifos-tenant': 'safaricom', 'office_id': context['templates_dict']['bloom2_office_id'], 'filters': "where bloom_version = '2'"},
            {'schema': 'bloomlive', 'mifos-tenant': 'default', 'office_id': context['templates_dict']['bloom1_office_id'], 'filters': "where bloom_version = '1'"},
            {'schema': 'solv_bat', 'mifos-tenant': 'tanda', 'office_id': context['templates_dict']['solv_office_id'], 'filters': None},
            {'schema': 'airtel_uganda_device_financing', 'mifos-tenant': 'devices', 'office_id': context['templates_dict']['airtel_uganda_office_id'],'filters': None}
        ]


        for src in sources:
            warehouse_clients = warehouse_hook.get_pandas_df(
                sql=f"""
                    SELECT
                      date_part('year', submitted_on_date) as "year", date_part('month', submitted_on_date) as "month", 
                      count(*) as total_count
                    FROM
                      {src['schema']}.client_dimension {src['filters'] if src['filters'] is not None else ''}
                    group by date_part('year', submitted_on_date), date_part('month', submitted_on_date)
                    order by date_part('year', submitted_on_date) desc, date_part('month', submitted_on_date) desc
                """
            )

            mifos_clients = mifos_hook.get_pandas_df(
                sql=f"""
                    select 
                        year(mc.submittedon_date) as 'year', month(mc.submittedon_date) as 'month', 
                        count(*) as total_count
                    from `mifostenant-{src['mifos-tenant']}`.m_client mc
                    where office_id = {src['office_id']}
                    group by year(mc.submittedon_date), month(mc.submittedon_date)
                    order by year(mc.submittedon_date) desc, month(mc.submittedon_date) desc
                """
            )

            merged = pd.merge(
                mifos_clients,
                warehouse_clients,
                on=["year", "month"],
                suffixes=("_mifos", "_warehouse")
            )

            merged['diff_count'] = merged['total_count_mifos'] - merged['total_count_warehouse']
            merged['updated_on_timestamp'] = datetime.datetime.utcnow()
            if src['filters'] is not None and 'bloom_version' in src['filters']:
                src['schema'] = f"""{src['schema']}{src['filters'].split(' ')[-1].replace("'", "")}""".strip()
            merged['warehouse_schema'] = src['schema']

            inserts = []
            for i, r in merged.iterrows():
                inserts.append(f"""
                    INSERT INTO monitoring.mifos_vs_warehouse_clients (
                        year, month, total_count_mifos, diff_count, total_count_warehouse,
                        updated_on_timestamp, warehouse_schema
                    ) VALUES (
                        {r['year']}, {r['month']}, {r['total_count_mifos']}, {r['diff_count']}, {r['total_count_warehouse']},
                        '{r['updated_on_timestamp']}', '{r['warehouse_schema']}'
                    ) ON DUPLICATE KEY UPDATE
                        total_count_mifos = VALUES(total_count_mifos),
                        diff_count = VALUES(diff_count),
                        total_count_warehouse = VALUES(total_count_warehouse),
                        updated_on_timestamp = VALUES(updated_on_timestamp);
                """.strip())

            airflow_hook.run(sql=inserts)

    def get_loan_summaries(**context):
        sources = [
            {'schema': 'bloomlive', 'view': 'loans_fact_table_materialized_summary_view', 'filters': "and bloom_version = '1'", 'mifos-tenant': 'default'},
            {'schema': 'bloomlive', 'view': 'loans_fact_table_materialized_summary_view', 'filters': "and bloom_version = '2'", 'mifos-tenant': 'safaricom'},
            {'schema': 'solv_bat', 'view': 'loans_fact_table_summary_view', 'filters': None, 'mifos-tenant': 'tanda'}
        ]

        for src in sources:
            warehouse_loans = warehouse_hook.get_pandas_df(
                sql=f"""
                    SELECT
                      date_part('year', disbursed_on_date) as "year", 
                        date_part('month', disbursed_on_date) as "month", 
                        count(*) as total_count,
                        sum(principal_disbursed) as sum_principal_disbursed,
                        sum(principal_outstanding) as sum_principal_outstanding,
                        sum(case when loan_status = 600 then 1 else 0 end) as count_loan_status_600,
                        sum(case when loan_status = 300 then 1 else 0 end) as count_loan_status_300
                    FROM
                      {src['schema']}.{src['view']} 
                    where disbursed_on_date is not null {src['filters'] if src['filters'] is not None else ''}
                    group by date_part('year', disbursed_on_date), date_part('month', disbursed_on_date)
                    order by date_part('year', disbursed_on_date) desc, date_part('month', disbursed_on_date) desc
                """
            )

            product_ids = [x[0] for x in warehouse_hook.get_records(
                sql=f"""
                    SELECT mifos_id as product_mifos_id
                    FROM {src['schema']}.product_dimension
                """
            )]
            mifos_loans = mifos_hook.get_pandas_df(
                sql=f"""
                    select
                        year(ml.disbursedon_date) as 'year',
                        month(ml.disbursedon_date) as 'month',
                        count(*) as total_count,
                        sum(ml.principal_disbursed_derived) as sum_principal_disbursed,
                        sum(ml.principal_outstanding_derived) as sum_principal_outstanding,
                        sum(case when loan_status_id = 600 then 1 else 0 end) as count_loan_status_600,
                        sum(case when loan_status_id = 300 then 1 else 0 end) as count_loan_status_300
                    from `mifostenant-{src['mifos-tenant']}`.m_loan ml
                    where disbursedon_date is not null
                    and ml.product_id in ({', '.join("{0}".format(w) for w in product_ids)})
                    group by year(ml.disbursedon_date), month(ml.disbursedon_date)
                    order by year(ml.disbursedon_date) desc, month(ml.disbursedon_date) desc
                """
            )

            merged = pd.merge(
                mifos_loans,
                warehouse_loans,
                on=["year", "month"],
                suffixes=("_mifos", "_warehouse")
            )

            merged['diff_count'] = merged['total_count_mifos'] - merged['total_count_warehouse']
            merged['diff_sum_principal_disbursed'] = merged['sum_principal_disbursed_mifos'] - merged['sum_principal_disbursed_warehouse']
            merged['diff_sum_principal_outstanding'] = merged['sum_principal_outstanding_mifos'] - merged['sum_principal_outstanding_warehouse']
            merged['diff_count_loan_status_300'] = merged['count_loan_status_300_mifos'] - merged['count_loan_status_300_warehouse']
            merged['diff_count_loan_status_600'] = merged['count_loan_status_600_mifos'] - merged['count_loan_status_600_warehouse']
            merged['updated_on_timestamp'] = datetime.datetime.utcnow()
            merged['warehouse_schema'] = src['schema']

            inserts = []
            for i, r in merged.iterrows():
                inserts.append(f"""
                    INSERT INTO monitoring.mifos_vs_warehouse_loans (
                        year, month, total_count_mifos, diff_count, total_count_warehouse,
                        sum_principal_disbursed_mifos, diff_sum_principal_disbursed, sum_principal_disbursed_warehouse,
                        sum_principal_outstanding_mifos, diff_sum_principal_outstanding, sum_principal_outstanding_warehouse,
                        count_loan_status_300_mifos, diff_count_loan_status_300, count_loan_status_300_warehouse,
                        count_loan_status_600_mifos, diff_count_loan_status_600, count_loan_status_600_warehouse,
                        updated_on_timestamp, warehouse_schema
                    ) VALUES (
                        {r['year']}, {r['month']}, {r['total_count_mifos']}, {r['diff_count']}, {r['total_count_warehouse']},
                        {r['sum_principal_disbursed_mifos']}, {r['diff_sum_principal_disbursed']}, {r['sum_principal_disbursed_warehouse']},
                        {r['sum_principal_outstanding_mifos']}, {r['diff_sum_principal_outstanding']}, {r['sum_principal_outstanding_warehouse']},
                        {r['count_loan_status_300_mifos']}, {r['diff_count_loan_status_300']}, {r['count_loan_status_300_warehouse']},
                        {r['count_loan_status_600_mifos']}, {r['diff_count_loan_status_600']}, {r['count_loan_status_600_warehouse']},
                        '{r['updated_on_timestamp']}','{r['warehouse_schema']}'
                    ) ON DUPLICATE KEY UPDATE
                        total_count_mifos = VALUES(total_count_mifos),
                        diff_count = VALUES(diff_count),
                        total_count_warehouse = VALUES(total_count_warehouse),
                        sum_principal_disbursed_mifos = VALUES(sum_principal_disbursed_mifos),
                        diff_sum_principal_disbursed = VALUES(diff_sum_principal_disbursed),
                        sum_principal_disbursed_warehouse = VALUES(sum_principal_disbursed_warehouse),
                        sum_principal_outstanding_mifos = VALUES(sum_principal_outstanding_mifos),
                        diff_sum_principal_outstanding = VALUES(diff_sum_principal_outstanding),
                        sum_principal_outstanding_warehouse = VALUES(sum_principal_outstanding_warehouse),
                        count_loan_status_300_mifos = VALUES(count_loan_status_300_mifos),
                        diff_count_loan_status_300 = VALUES(diff_count_loan_status_300),
                        count_loan_status_300_warehouse = VALUES(count_loan_status_300_warehouse),
                        count_loan_status_600_mifos = VALUES(count_loan_status_600_mifos),
                        diff_count_loan_status_600 = VALUES(diff_count_loan_status_600),
                        count_loan_status_600_warehouse = VALUES(count_loan_status_600_warehouse),
                        updated_on_timestamp = VALUES(updated_on_timestamp);
                """.strip())

            airflow_hook.run(sql=inserts)

    def get_bloom_whitelist_summaries(**context):
        warehouse_whitelist = warehouse_hook.get_pandas_df(
            sql="""
                SELECT SUM(CASE WHEN blacklist_flag = 1 THEN 1 ELSE 0 END) AS blacklist_flag_1_count,
                SUM(CASE WHEN blacklist_flag = 0 THEN 1 ELSE 0 END) AS blacklist_flag_0_count,
                SUM(final_21_limit) AS sum_final_21_limit, SUM(final_7_limit) AS sum_final_7_limit,
                SUM(final_1_limit) AS sum_final_1_limit FROM bloomlive.scoring_results
                WHERE model_version = (SELECT MAX(model_version) FROM bloomlive.scoring_results);

            """
        )
        safaricom_whitelist = airflow_hook.get_pandas_df(
            sql="""
                SELECT SUM(CASE WHEN Asante_Blacklist_Flag = 1 THEN 1 ELSE 0 END) AS blacklist_flag_1_count,
                SUM(CASE WHEN Asante_Blacklist_Flag = 0 THEN 1 ELSE 0 END) AS blacklist_flag_0_count,
                SUM(Asante_Credit_Limit_21_Day) AS sum_final_21_limit, SUM(Asante_Credit_Limit_7_Day) AS sum_final_7_limit,
                SUM(Asante_Credit_Limit_1_Day) AS sum_final_1_limit 
                FROM bloom_pipeline.current_whitelist_mock
            """
        )
        joined = safaricom_whitelist.join(
            warehouse_whitelist,
            lsuffix="_safaricom",
            rsuffix="_warehouse",
            how='outer'
        )
        joined['id'] = 1
        joined['diff_count_blacklist_0'] = joined['blacklist_flag_0_count_warehouse'] - joined['blacklist_flag_0_count_safaricom']
        joined['diff_count_blacklist_1'] = joined['blacklist_flag_1_count_warehouse'] - joined['blacklist_flag_1_count_safaricom']
        joined['diff_sum_final_1_limit'] = joined['sum_final_1_limit_warehouse'] - joined['sum_final_1_limit_safaricom']
        joined['diff_sum_final_7_limit'] = joined['sum_final_7_limit_warehouse'] - joined['sum_final_7_limit_safaricom']
        joined['diff_sum_final_21_limit'] = joined['sum_final_21_limit_warehouse'] - joined['sum_final_21_limit_safaricom']

        inserts = []
        for i, r in joined.iterrows():
            inserts.append(f"""
                INSERT INTO monitoring.safaricom_vs_warehouse_whitelist (
                    id, blacklist_flag_1_count_safaricom, blacklist_flag_0_count_safaricom, sum_final_21_limit_safaricom, 
                    sum_final_7_limit_safaricom, sum_final_1_limit_safaricom, blacklist_flag_1_count_warehouse,
                    blacklist_flag_0_count_warehouse, sum_final_21_limit_warehouse, sum_final_7_limit_warehouse, 
                    sum_final_1_limit_warehouse, diff_count_blacklist_0, diff_count_blacklist_1, diff_sum_final_1_limit, 
                    diff_sum_final_7_limit, diff_sum_final_21_limit
                ) VALUES (
                    {r['id']}, {r['blacklist_flag_1_count_safaricom']}, {r['blacklist_flag_0_count_safaricom']}, {r['sum_final_21_limit_safaricom']}, 
                    {r['sum_final_7_limit_safaricom']}, {r['sum_final_1_limit_safaricom']}, {r['blacklist_flag_1_count_warehouse']}, 
                    {r['blacklist_flag_0_count_warehouse']}, {r['sum_final_21_limit_warehouse']}, {r['sum_final_7_limit_warehouse']}, 
                    {r['sum_final_1_limit_warehouse']}, {r['diff_count_blacklist_0']}, {r['diff_count_blacklist_1']}, {r['diff_sum_final_1_limit']}, 
                    {r['diff_sum_final_7_limit']}, {r['diff_sum_final_21_limit']}
                ) ON DUPLICATE KEY UPDATE
                    id = VALUES(id),
                    blacklist_flag_1_count_safaricom = VALUES(blacklist_flag_1_count_safaricom),
                    blacklist_flag_0_count_safaricom = VALUES(blacklist_flag_0_count_safaricom),
                    sum_final_21_limit_safaricom = VALUES(sum_final_21_limit_safaricom),
                    sum_final_7_limit_safaricom = VALUES(sum_final_7_limit_safaricom),
                    sum_final_1_limit_safaricom = VALUES(sum_final_1_limit_safaricom),
                    blacklist_flag_1_count_warehouse = VALUES(blacklist_flag_1_count_warehouse),
                    blacklist_flag_0_count_warehouse = VALUES(blacklist_flag_0_count_warehouse),
                    sum_final_21_limit_warehouse = VALUES(sum_final_21_limit_warehouse),
                    sum_final_7_limit_warehouse = VALUES(sum_final_7_limit_warehouse),
                    sum_final_1_limit_warehouse = VALUES(sum_final_1_limit_warehouse),
                    diff_count_blacklist_0 = VALUES(diff_count_blacklist_0),
                    diff_count_blacklist_1 = VALUES(diff_count_blacklist_1),
                    diff_sum_final_1_limit = VALUES(diff_sum_final_1_limit),
                    diff_sum_final_7_limit = VALUES(diff_sum_final_7_limit),
                    diff_sum_final_21_limit = VALUES(diff_sum_final_21_limit);
            """.strip())
        airflow_hook.run(sql=inserts)


    common_params = {
        'solv_office_id': '{{solv_office_id}}', 'bloom1_office_id': '{{bloom1_office_id}}',
        'bloom2_office_id': '{{bloom2_office_id}}', 'airtel_uganda_office_id': '{{airtel_uganda_office_id}}'
    }
    t1 = PythonOperator(
        task_id='get_transaction_summaries',
        provide_context=True,
        python_callable=get_transaction_summaries,
    )
    t2 = PythonOperator(
        task_id='get_client_summaries',
        provide_context=True,
        templates_dict=common_params,
        python_callable=get_client_summaries,
    )
    t3 = PythonOperator(
        task_id='get_loan_summaries',
        provide_context=True,
        python_callable=get_loan_summaries,
    )
    t4 = PythonOperator(
        task_id='get_bloom_whitelist_summaries',
        provide_context=True,
        python_callable=get_bloom_whitelist_summaries,
    )

    t1 >> t2 >> t3 >> t4


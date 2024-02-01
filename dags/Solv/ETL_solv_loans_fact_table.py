import os
import sys
import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, el_loans_fact_table

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_solv_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 10, 21, 10, 00, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into Solv loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1,
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.tanda.loans_fact_table following SCD type 2
    DAG is set to run daily at 6pm.

    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_loan</li>
    <li>Check for existing loan instances in ubuntu.tanda.loans_fact_table</li>
    <li>Update existing instances in ubuntu.tanda.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.tanda.loans_fact_table</li>
    </ol>
    """


    def extract_load_loans_fact_table(**context) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse
        :param context: dictionary of predefined values
        :return: None
        """

        el_loans_fact_table(mifos_tenant='tanda', warehouse_schema='solv_bat')


    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='solv_bat',
            filters=None
        )


    def el_m_loan_charge() -> None:
        """
        Extracts loans from m_charge, m_loan_charge and load them into warehouse
        :return: None
        """
        mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-tanda')
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                      schema='afsg_ds_prod_postgresql_dwh')

        charges = mifos_hook.get_pandas_df(
            sql="""
                    SELECT
                        mlc.loan_id as loan_mifos_id,
                        mlc.charge_id as charge_id,
                        mlc.due_for_collection_as_of_date as due_for_collection_as_of_date,
                        mlc.calculation_percentage as calculation_percentage,
                        mlc.calculation_on_amount as calculation_on_amount,
                        mlc.charge_amount_or_percentage as charge_amount_or_percentage,
                        mlc.amount as amount,
                        mlc.amount_paid_derived as amount_paid_derived,
                        mlc.amount_waived_derived as amount_waived_derived,
                        mlc.amount_writtenoff_derived as amount_writtenoff_derived,
                        mlc.amount_outstanding_derived as amount_outstanding_derived,
                        mc.currency_code as currency_code, 
                        mc.fee_on_day as fee_on_day,
                        mc.fee_interval as fee_interval,
                        mc.fee_on_month as fee_on_month,
                        mc.fee_frequency as fee_frequency,
                        mc.income_or_liability_account_id as income_or_liability_account_id,
                        mc.tax_group_id as tax_group_id,
                        mc.amount as charge_amount,
                        mc.name as charge_name
                    from `mifostenant-tanda`.m_loan_charge mlc
                    left join `mifostenant-tanda`.m_charge mc
	                    on mlc.charge_id = mc.id
                    """
        ).replace({None: np.NAN})

        warehouse_hook.run(sql=""" DELETE FROM solv_bat.loan_charges """) # overwrite data

        charges = charges[['loan_mifos_id', 'charge_id', 'due_for_collection_as_of_date', 'calculation_percentage',
                           'calculation_on_amount', 'charge_amount_or_percentage', 'amount', 'charge_amount',
                           'amount_paid_derived', 'amount_waived_derived', 'amount_writtenoff_derived',
                           'amount_outstanding_derived', 'currency_code', 'fee_on_day', 'fee_interval',
                           'fee_on_month', 'fee_frequency', 'income_or_liability_account_id',
                           'tax_group_id', 'charge_name']]

        warehouse_hook.insert_rows(
            table=f'solv_bat.loan_charges',
            target_fields=charges.reindex(sorted(charges.index)).columns.tolist(),
            replace=False,
            rows=tuple(charges.reindex(sorted(charges.index)).replace({np.NAN: None}).itertuples(index=False)),
            commit_every=1000
        )


    t1 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )

    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )

    t3 = PythonOperator(
        task_id='extract_load_loan_charges',
        provide_context=True,
        python_callable=el_m_loan_charge,
    )

    t1 >> t2 >> t3

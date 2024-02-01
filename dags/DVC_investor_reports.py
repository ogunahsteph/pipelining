import os
import sys
import datetime
import pendulum
import calendar
import pandas as pd
from io import BytesIO
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import on_failure
from utils.office365_api import upload_file

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'DVC_investor_reports',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 0 1 * *', # First day of every month
        start_date=datetime.datetime(2023, 8, 18, 15, 00, tzinfo=local_tz),
        tags=['reports'],
        description='Trigger DVC investor reports',
        user_defined_macros=default_args
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Triggers DVC data pipelines for investor reports

    #### Actions
    <ol>
    <li>Trigger DVC investor reports data pipeline</li>
    <li>Upload DVC generated reports to sharepoint</li>
    </ol>
    """
    def upload_dvc_loan_tape_report_to_sharepoint(**context):
        local_file_path = f"{Variable.get('DVC_afsg_lender_reports_project_path')}/data/processed/loan_tape_requests.xlsx"

        # Get the current date
        # Get today's date
        current_date = datetime.datetime.today()

        # Calculate the previous month and year
        previous_month = (current_date.month - 2) % 12 + 1
        previous_year = current_date.year if previous_month <= current_date.month else current_date.year - 1

        # Create a BytesIO buffer to hold the Excel data
        excel_buffer = BytesIO()

        # Read all sheets from the Excel file into a dictionary of DataFrames
        all_sheets = pd.read_excel(local_file_path, sheet_name=None)

        # Write all sheets to the BytesIO buffer as an Excel file
        with pd.ExcelWriter(excel_buffer) as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        upload_file(file_name=f"{calendar.month_name[previous_month]} {previous_year} loan_tape_requests.xlsx",
                    file_bytes=excel_buffer.getvalue(), site={
            'name': 'AsanteFinanceGroup-AsanteDataScience',
            'id': '78d38e76-5dbf-4234-b605-3f3fb44eaf63',
            'description': 'End of Month Loan Tape reports',
            'folder': '01YNP2I5EMGYJMCQSPPBB2E73ENL3KKMUY'
        })


    t1 = BashOperator(
        task_id='trigger_dvc_loan_tape_report',
        bash_command=f"""
            cd {Variable.get('DVC_afsg_lender_reports_project_path')};
            source env/bin/activate;
            git checkout develop;
            cd pipelines/loan_tape/;
            bash fetch_loan_tape_pipeline_data.sh;
            bash run_loan_tape_pipeline.sh
        """,
        retries=0
    )
    t2 = PythonOperator(
        task_id='upload_dvc_loan_tape_report_to_sharepoint',
        provide_context=True,
        python_callable=upload_dvc_loan_tape_report_to_sharepoint
    )
    t1 >> t2


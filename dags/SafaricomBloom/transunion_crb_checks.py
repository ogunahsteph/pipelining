import re
import os
import sys
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from io import StringIO
from datetime import timedelta
from xml.etree import ElementTree
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.common import on_failure
from utils.transunion_api import TransUnionApi
from utils.office365_api import get_access_token, get_children, read_file, upload_file

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='monitoring')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

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
        'TransUnion_CRB_checks',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2023, 7, 28, 12, 00, tzinfo=local_tz),
        tags=['data_checks'],
        description='Conduct CRB checks for Bloom clients',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### DAG SUMMARY

    This pipeline retrieves Credit Reference Bureau data for a given dataset from TransUnion CRB.
    The DAG is triggered manually.
    To connect to TransUnion, the DAG makes use of connection details specified in airflow varibles

    #### Actions
        1. Extract raw data from sharepoint
        2. Retrieve CRB data
        3. Store raw logs in "bloom_pipeline" database located at server 10.0.4.23 in the table "transunion_crb_checks". 
        4. Unpack and flatten results from the raw logs 
        4. Export output to sharepoint. If the product is product139, also store the output to the data warehouse table "bloomlive.transunion_high_velocity_crb_checks"

    ### Configuration Parameters
    The pipeline takes in two parameters; `file_name` and `product_name`.
    
    The `product_name` can only be one of two options; **"product139"** or **"product103"**.
     
    "product139" is the High Velocity product while "product103" is the Skip Trace product.
    The product name is **case sensitive**.
    
    The `file_name` is the name of a file in <a href="https://netorgft4232141.sharepoint.com/:f:/s/SafaricomBloom/Emr2JRC6M-ZHn5Y-w415M8IBDEmzoYpGfbPEeCcoF0tlww?e=CFSwHr" style="color: #007bff;">this sharepoint folder</a> of the 'Safaricom Bloom' site and should include the file extension.
    
    Examples;
    ```
     {"file_name": "Skip_Trace_2023_08_09.xlsx", "product_name": "product103"}
     
     {"file_name": "Bloom Merchants With Limits.xlsx", "product_name": "product139"}
    ```
    
    ### Input
    The DAG expects the file in the `file_name` parameter to have the following structure.
    1. It must be a valid Excel file (`.xlsx` or `.xls`).
    2. It must contain the following columns:
       1. `national_id`: The national ID of the individual (Mandatory).
       2. `first_name`: The first name of the individual.
       3. `middle_name`: The middle name of the individual.
       4. `surname`: The surname or last name of the individual.
       5. `mobile_number`: The mobile number of the individual (optional).
    
    For each record in the input file, the record must have at least two names of the expected 3 names (first_name, middle_name, last_name)
    
    ### Output
    The DAG stores raw logs in the database server 10.0.4.23 in the table "transunion_crb_checks"
    
    The DAG stores cleaned data in <a href="https://netorgft4232141.sharepoint.com/:f:/s/SafaricomBloom/EkYvhybsicJCht6on4Eci30BdffzM_i6SBB9SgwRXhVz4g?e=9jxqfj" style="color: #007bff;">this sharepoint folder</a>
    in a csv file
     
    The output file is named the same as the input file but with the word 'out' appended to it for example, if the input file is named 
     'Skip_Trace_2023_08_09.xlsx', the output file will be named 'Skip_Trace_2023_08_09_out.xlsx'
    
    If the product was "product139" then the cleaned data is also stored in the data warehouse. This is in the schema bloomlive, in the table transunion_high_velocity_crb_checks
    
    ### API Call Logic
    The source code for the API is in <a href="https://github.com/Asante-FSG/DataPipelines/blob/github_data_piplenines/utils/transunion_api.py" style="color: #007bff;">this Github file</a>

    To determine whether to retrieve data from the logs or call the TransUnion API, consider the following scenarios:
    
    1. **Same File, Same Product:**
       - If you run the same file with the same product as before, the results will be fetched from the existing logs for previous successful requests. 
       For previous failed requests and new national IDs, a call to the TransUnion API will be triggered for result retrieval 
    
    2. **Same File, Different Product:**
       - Running the same file with a different product will trigger a call to the TransUnion API for result retrieval.
    
    3. **Different File, Same National IDs:**
       - If you use a different file containing the same national IDs, the DAG will initiate a request to the TransUnion API.
    
    The decision-making process relies on the following SQL logic executed on mysql on server 10.0.4.23:3306:
    
    ```sql
    select national_id 
    from bloom_pipeline.transunion_crb_logs tcl 
    where menu = '{product_name}' and src = '{file_name}'
    ```
    
    """

    def convert_dict_keys_to_snake_case(input_dict):
        if not isinstance(input_dict, dict):
            return input_dict  # If input is not a dictionary, return the value as is

        output_dict = {}
        for key, value in input_dict.items():
            value = convert_dict_keys_to_snake_case(value)  # Recursive call for nested dictionaries
            # Use regular expression to replace uppercase letters with underscores and lowercase letters
            renamed_key = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', key).lower()
            output_dict[renamed_key] = value
        return output_dict


    def remove_non_required_keys(d):
        keys_to_remove = set()
        for k in d.keys():
            is_parent = any(k != key and key.startswith(k) for key in d.keys())
            if is_parent:
                keys_to_remove.add(k)
        return keys_to_remove


    def remove_repeated_word(input_str):
        words = input_str.split("_")
        cleaned_words = [word for idx, word in enumerate(words) if word not in words[:idx]]
        cleaned_str = "_".join(cleaned_words)
        return cleaned_str


    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


    def rename_dict_keys(d):
        renamed_dict = {remove_repeated_word(k): v for k, v in d.items()}
        return renamed_dict

    def xml_to_dict(element):
        result = {}
        if element.text:
            result[element.tag] = element.text
        else:
            result[element.tag] = ''
        for child in element:
            child_result = xml_to_dict(child)
            if child.tag in result:
                if isinstance(result[child.tag], list):
                    result[child.tag].append(child_result)
                else:
                    result[child.tag] = [result[child.tag], child_result]
            else:
                result[child.tag] = child_result
        return result

    def store_results(file_name: str, unpacked_data: pd.DataFrame, product_name: str) -> None:
        # Split the file name and extension
        file_name, file_extension = file_name.rsplit('.', 1)
        csv_buffer = StringIO()
        unpacked_data.to_csv(csv_buffer, index=False)

        upload_file(file_name=f'{file_name}_out.csv', file_bytes=csv_buffer.getvalue(), site={
            'name': 'Safaricom Bloom',
            'id': 'ea835111-82a4-467d-81ed-3c5994f4d899',
            'folder': '01YAZIWUKGF6DSN3EJYJBINXVIT6ARZC35'}
        )

        if product_name == 'product139':
            warehouse_hook.insert_rows(
                table='bloomlive.transunion_high_velocity_crb_checks',
                target_fields=unpacked_data.reindex(sorted(unpacked_data.index)).columns.tolist(),
                replace=True,
                rows=tuple(unpacked_data.reindex(sorted(unpacked_data.index)).replace({np.NAN: None}).itertuples(index=False, name=None)),
                replace_index=['national_id', 'src'],
                commit_every=200
            )

    def get_unpacked_data(rslt_dict, product_name):
        if rslt_dict['response_code'] == '200':
            national_id = rslt_dict['national_id']
            rslt_dict = rslt_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body'][
                '{http://ws.crbws.transunion.ke.co/}get%sResponse' % (product_name.capitalize())]['return']

            if product_name == 'product139':
                rslt_dict['header'].pop('header')
                rslt_dict['personalProfile'].pop('personalProfile')
                rslt_dict['scoreOutput'].pop('scoreOutput')
                rslt_dict['summary'].pop('summary')

                header = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['header']))
                personal_profile = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['personalProfile']))
                score_output = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['scoreOutput']))
                summary = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['summary']))

                combined_dict = {k: v for d in (header, personal_profile, score_output, summary) for k, v in d.items()}

            elif product_name == 'product103':
                rslt_dict['header'].pop('header')
                rslt_dict['personalProfile'].pop('personalProfile')

                header = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['header']))
                personal_profile = flatten_dict(convert_dict_keys_to_snake_case(rslt_dict['personalProfile']))

                try:
                    rslt_dict['phoneList'] = [rslt_dict['phoneList']] if type(rslt_dict['phoneList']) != list else rslt_dict['phoneList']
                except KeyError:
                    rslt_dict['phoneList'] = []

                try:
                    rslt_dict['postalAddressList'] = [rslt_dict['postalAddressList']] if type(rslt_dict['postalAddressList']) != list else rslt_dict['postalAddressList']
                except KeyError:
                    rslt_dict['postalAddressList'] = []

                try:
                    rslt_dict['physicalAddressList'] = [rslt_dict['physicalAddressList']] if type(rslt_dict['physicalAddressList']) != list else rslt_dict['physicalAddressList']
                except KeyError:
                    rslt_dict['physicalAddressList'] = []

                phone_list = {'phone_list': []}
                postal_address_list = {'postal_address_list': []}
                physical_address_list = {'physical_address_list': []}

                for x in rslt_dict['phoneList']:
                    x.pop('phoneList')
                    phone_list['phone_list'].append(rename_dict_keys(flatten_dict(convert_dict_keys_to_snake_case(x))))

                for x in rslt_dict['postalAddressList']:
                    x.pop('postalAddressList')
                    postal_address_list['postal_address_list'].append(rename_dict_keys(flatten_dict(convert_dict_keys_to_snake_case(x))))

                for x in rslt_dict['physicalAddressList']:
                    x.pop('physicalAddressList')
                    physical_address_list['physical_address_list'].append(rename_dict_keys(flatten_dict(convert_dict_keys_to_snake_case(x))))

                combined_dict = {k: v for d in (header, personal_profile, physical_address_list, postal_address_list, phone_list) for k, v in d.items()}


            renamed_dict = rename_dict_keys(combined_dict)

            for key in remove_non_required_keys(renamed_dict):
                renamed_dict.pop(key)

            if 'national_id' not in renamed_dict:
                renamed_dict['national_id'] = national_id

            return renamed_dict

    def get_results_dict(rslt, product_name):
        content_str = rslt['raw_response']
        # Parse the XML content using ElementTree
        root = ElementTree.fromstring(content_str)
        result_dict = xml_to_dict(root)
        result_dict['national_id'] = rslt['national_id']
        result_dict['timestamp'] = rslt['timestamp'].isoformat()
        result_dict['raw_response'] = rslt['raw_response']
        result_dict['raw_request'] = rslt['raw_request']
        result_dict['response_code'] = result_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body'][
            '{http://ws.crbws.transunion.ke.co/}get%sResponse' % (product_name.capitalize())]['return']['responseCode'][
            'responseCode']

        return result_dict

    def get_report(api: TransUnionApi, chunk: pd.DataFrame, product_name: str) -> list:
        results = []
        for i, r in chunk.iterrows():
            rslt = api.get_product(
                national_id=r['national_id'],
                first_name=r['first_name'],
                middle_name=r['middle_name'],
                surname=r['surname'],
                mobile_number=r['mobile_number'],
                product_name=product_name
            )
            results.append(get_results_dict(rslt=rslt, product_name=product_name))
        return results

    def flatten_lists(df: pd.DataFrame, product_name: str):
        if product_name == 'product103':
            # List of column names to process
            columns_to_process = ['phone_list', 'postal_address_list', 'physical_address_list']

            # Process each column
            for column_name in columns_to_process:
                for idx, row in df.iterrows():
                    data_list = row[column_name]
                    if isinstance(data_list, list):
                        for i, data in enumerate(data_list, start=1):
                            for key, value in data.items():
                                new_column_name = f'{column_name}{i}_{key}'
                                df.at[idx, new_column_name] = value
                    elif isinstance(data_list, dict):
                        for key, value in data_list.items():
                            new_column_name = f'{column_name}1_{key}'
                            df.at[idx, new_column_name] = value

            df.drop(columns=columns_to_process, inplace=True)

    def get_credit_report(**context):
        file_name = context['dag_run'].conf.get('file_name')
        product_name = context['dag_run'].conf.get('product_name')

        if product_name not in ('product139', 'product103'):
            raise ValueError("Invalid product name. Allowed values are 'product139' or 'product103'.")

        # Define the payload dictionary containing information about the site and folder to extract data from
        payload = {
            'site': {'name': 'Safaricom Bloom', 'id': 'ea835111-82a4-467d-81ed-3c5994f4d899'},
            'folder': {'path': '/Transunion Credit Reports/Input/', 'id': '01YAZIWULK6YSRBORT4ZDZ7FR6YOGXSM6C'}
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()

        files = get_children(token=access_token, site_id=payload['site']['id'], item_id=payload['folder']['id'])

        try:
            file = [d for d in files if d.get('name') == file_name][0]
        except IndexError:
            raise Exception(f'File with name "{file_name}" NOT FOUND on sharepoint')

        # read input file
        dt = pd.read_excel(read_file(site=payload['site']['id'], file_id=file['id'], access_token=access_token))
        dt.drop_duplicates(subset=['national_id'], inplace=True) # drop duplicates
        dt['national_id'] = dt['national_id'].astype(str)
        dt = dt[dt['national_id'].str.contains(r'^[0-9\.]+$') & ~(dt['national_id'].str.len() > 8)]
        dt['national_id'] = dt['national_id'].astype(int)

        already_checked = airflow_hook.get_pandas_df(
            f"""select national_id from bloom_pipeline.transunion_crb_logs tcl where menu = '{product_name}' and src='{file_name}' and trim(response_code) = '200'"""
        )

        dt = dt[~dt.astype({'national_id': int})['national_id'].isin(
            already_checked.astype({'national_id': int})['national_id'].tolist())]

        if dt.shape[0] > 0:
            api = TransUnionApi(env='prod')
            dt['result_dict'] = np.NAN
            response_code_mapping = {
                '101': 'General Authentication Error', '102': 'Invalid Infinity Code',
                '103': 'Invalid Authentication Credentials', '104': 'Password expired',
                '106': 'Access Denied', '109': 'Account locked',
                '200': 'Product request processed successfully',
                '202': 'Credit Reference Number not found',
                '203': 'Multiple Credit Reference Number Found',
                '204': 'Invalid report reason', '209': 'Invalid Sector ID',
                '301': 'Insufficient Credit', '402': 'Required input missing',
                '403': 'General Application Error', '404': 'Service temporarily unavailable',
                '408': 'Unable to verify National ID'
            }

            chunk_size = 20

            # Calculate the total number of chunks needed
            num_chunks = (len(dt) + chunk_size - 1) // chunk_size

            # Iterate through the DataFrame in chunks of 20 rows
            for chunk_num in range(num_chunks):
                start_idx = chunk_num * chunk_size
                end_idx = min((chunk_num + 1) * chunk_size, len(dt))
                chunk = dt.iloc[start_idx:end_idx]
                results = get_report(api=api, chunk=chunk, product_name=product_name)

                results = pd.DataFrame(results)
                results['failure_reason'] = results['response_code'].apply(
                    lambda x: response_code_mapping[x] if x != '200' else None
                )
                results['menu'] = product_name
                results['src'] = file['name']

                airflow_hook.insert_rows(
                    table='bloom_pipeline.transunion_crb_logs',
                    target_fields=[
                        'national_id', 'request_time', 'raw_request', 'raw_response', 'response_code', 'failure_reason',
                        'menu', 'src'
                    ],
                    replace=False,
                    rows=tuple(results[[
                        'national_id', 'timestamp', 'raw_request', 'raw_response', 'response_code', 'failure_reason',
                        'menu', 'src'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=200
                )
        else:
            logging.warning("No national IDS to check")

    def unpack_raw_responses(**context):
        file_name = context['dag_run'].conf.get('file_name')
        product_name = context['dag_run'].conf.get('product_name')

        results = airflow_hook.get_pandas_df(
            sql=f"""
                select raw_response, national_id, request_time as "timestamp", raw_request from bloom_pipeline.transunion_crb_logs tcl 
                where menu = '{product_name}' and src='{file_name}'
            """
        )
        results = [get_results_dict(r, product_name=product_name) for i, r in results.iterrows()]

        unpacked_data = pd.DataFrame(
            [x for x in [get_unpacked_data(r, product_name) for r in results] if x is not None])

        if unpacked_data.shape[0] > 0:
            flatten_lists(unpacked_data, product_name=product_name)
            unpacked_data['src'] = file_name
            store_results(
                file_name=file_name,
                unpacked_data=unpacked_data,
                product_name=product_name
            )

    t1 = PythonOperator(
        task_id='get_credit_report',
        provide_context=True,
        python_callable=get_credit_report,
    )
    t2 = PythonOperator(
        task_id='unpack_raw_responses',
        provide_context=True,
        python_callable=unpack_raw_responses,
    )
    t1 >> t2

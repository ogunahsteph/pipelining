import io
import json
import logging
import zipfile
import requests
import datetime
import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')


def get_access_token():
    response = requests.post(
        url='https://login.microsoftonline.com/asantefinancegroup.com/oauth2/v2.0/token',
        data={
            'client_id': Variable.get('microsoft_graph_client_id'),
            'scope': 'https://graph.microsoft.com/.default',
            'client_secret': Variable.get('microsoft_graph_client_secret'),
            'grant_type': 'client_credentials'
        }
    )
    return json.loads(response.text)['access_token']

def get_sites(token: str) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)

def read_file(access_token: str, site: str, file_id: str) -> bytes:
    """
    streams files from sharepoint site drive

    param access_token: the access token for microsoft graph api
    param site: the identifier for the sharepoint site
    param file_id: the identifier for the sharepoint file
    param dest: the location into which to save the downloaded file. Should also include the file name
                e.g /home/henrykuria/data/Saf/Bloom/Safaricom Bloom 2.0 Till Suspension_Master Summary.xlsx
    """

    def get_response():
        return requests.get(
            url=f"https://graph.microsoft.com/v1.0/sites/{site}/drive/items/{file_id}/content",
            headers={
                'Authorization': f"Bearer {access_token}",
                'Accept': "application/octet-stream"
            }
        )

    response = get_response()

    return response.content

def get_lists(token: str, site: str) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/{site}/lists/",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)

def get_folders(token: str, site: str, list) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/{site}/lists/{list}/drive/root/children",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)


def get_children(token: str, site_id: str, item_id: str) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/children/",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)["value"]


def get_drives(token: str, site: str, file_path: str) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/{site}/drive/root:{file_path}",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)


def upload_file(file_bytes: str, file_name: str, site: dict):
    import json
    import requests

    access_token = get_access_token()

    url = f"https://graph.microsoft.com/v1.0/sites/{site['id']}/drive/items/{site['folder']}:/{file_name}:/content"

    file_size = len(file_bytes)

    if file_size <= 4000000:
        response = requests.put(
            url=url,
            headers={
                'Authorization': f"Bearer {access_token}",
                'Accept': "application/json",
                'Content-Type': "text/plain"
            },
            data=file_bytes
        )
        return json.loads(response.text)
    elif file_size > 4000000:
        res = requests.post(
            url=url.replace('/content', '/createUploadSession'),
            headers={
                'Authorization': f"Bearer {access_token}",
                'Content-Type': "application/json"
            },
            json=json.dumps({
                "item": {
                    "@odata.type": "microsoft.graph.driveItemUploadableProperties",
                    "@microsoft.graph.conflictBehavior": "replace",
                    "name": f'{file_name}'
                }
            })
        )
        if res.status_code == 200:
            response = requests.put(
                url=f'{json.loads(res.text)["uploadUrl"]}',
                headers={
                    'Content-Length': f'{file_size}',
                    'Content-Range': f'bytes 0-{file_size - 1}/{file_size}',
                },
                data=file_bytes
            )

            return json.loads(response.text)


def get_file(token: str, site: str, file_path: str) -> dict:
    return json.loads(requests.get(
        url=f"https://graph.microsoft.com/v1.0/sites/{site}/drive/root:{file_path}",
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': "application/json"
        }
    ).text)


# if __name__ == '__main__':
    # sites = [
    #     {'name': 'DSGrafanaTest', 'id': '2be7256e-c06d-40e9-9fe5-0d15b48ead72'},
    #     {'name': 'AsanteFinanceGroup', 'id': '9163720e-177b-48e1-b701-1d4953005bfd'},
    #     {'name': 'Safaricom Data Dumps', 'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be'},
    #     {'name': 'Asante Data Sharing', 'id': '342e7677-1dbc-4a61-9e48-190d29f920d9'},
    #     {'name': 'AsanteFinanceGroup-AsanteDataScience', 'id': '78d38e76-5dbf-4234-b605-3f3fb44eaf63'},
    #     {'name': 'Bloom Automated Data Posting Reports', 'id': '3ab56d3d-b9b2-43ae-a496-633ee8fa1158'},
    #     {'name': 'AsanteBloomDefaultersList', 'id': 'b91dfc87-5643-4a6f-90d3-af017d03896c'},
    #     {'name': 'Safaricom Bloom', 'id': 'ea835111-82a4-467d-81ed-3c5994f4d899'},
    #     {'name': 'DataDumps', 'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be'}
    # ]

#     folders = [
#         {'path': '/Engineering/TEP/', 'parent_id': '0153UIAVMDRHFZPPGLP5CZG3SmAKB3J7VI2'},
#         {'path': '/', 'id': '01BDVLPEV6Y2GOVW7725BZO354PWSELRRZ'},
#         {'path': '/', 'id': '01YAZIWUOBN27FPSG2IBCKIACCQ5VLDXK7'},
#         {'id': '01YAZIWUPZ5IGJOV7PWBHIVYZCC6C5XVMN'}
#     ]
#
    # access_token = get_access_token()
    # print(json.dumps(get_folders(access_token, sites[-1]['id'], 'ec489717-9583-44ad-9890-f316099f302e'), indent=2))
    # print(json.dumps(get_children(token=access_token, site_id=sites[7]['id'], item_id=folders[3]['id']), indent=2))


    # print(json.dumps(get_drives(token=access_token, site=sites[7]['id'], file_path='/Bloom Collections - Inhouse/Manual Reconciliations/Repayments/'), indent=2))
    # print(json.dumps(upload_daily_collections_summary_report(token=access_token, site=sites[1]['id'], parent_id=files[1]['parent_id']), indent=2))
    # print(json.dumps(upload_daily_disbursements_file(token=access_token, site=sites[2]['id'], parent_id=files[2]['parent_id']), indent=2))

    # print(json.dumps(get_drives(token=access_token, site=sites[5]['id'], file_path='/Email attachments'), indent=2))
    # print(download_bloom_sms_campaign_templates(token=access_token, site=sites[0]['id'], file_id=files[0]['id']))

    # print(json.dumps(get_lists(token=access_token, site=sites[-1]['id']), indent=2))

    # print(json.dumps(upload_daily_collections_summary_report(token=access_token, site=sites[1]['id'], parent_id=files[1]['parent_id']), indent=2))
    # print(upload_daily_collections_summary_report(token=access_token, site=sites[1]['id'], parent_id=files[1]['parent_id'])['webUrl'])
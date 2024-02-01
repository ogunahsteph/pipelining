import requests
import json
import pandas as pd
from airflow.models import Variable


def get_transactions(username:str, password:str, base_url: str, loan_id: int, offset=0, limit=100) -> pd.DataFrame:
    # Set the API endpoint URL
    url = f"{base_url}/{id}/transactions"

    # Set the request headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer your-api-key"
    }

    # Make the request
    response = requests.get(url, headers=headers, auth=requests.auth.HTTPBasicAuth(username, password))

    # Check the status code
    if response.status_code == 200:
        # Print the response data
        print(response.json())
    else:
        # Print the error message
        print(f"Request failed: {response.status_code} {response.reason}")


def get_loan_products(username:str, password:str, base_url: str, offset=0, limit=100) -> pd.DataFrame:
    headers = {'Accept': 'application/vnd.mambu.v2+json'}

    response = requests.get(
        f'{base_url}/loanproducts',
        params={
            'offset': offset,
            'limit': limit
        },
        headers=headers,
        auth=requests.auth.HTTPBasicAuth(username, password)
    )

    return pd.DataFrame(response.json())


def get_loans(username: str, password: str, base_url: str, branch_key: str, centre_key: str, offset=0, limit=100) -> pd.DataFrame:
    import requests

    # Set the API endpoint URL
    url = f"{base_url}/loans"

    # Set the request headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer your-api-key"
    }

    # Set the request parameters (optional)
    params = {
        "offset": 0,  # The number of loans to skip (default: 0)
        "limit": 50,  # The maximum number of loans to retrieve (default: 50)
        "fullDetails": False,  # Whether to include full loan details (default: False)
    }

    # Make the request
    response = requests.get(url, headers=headers, params=params, auth=requests.auth.HTTPBasicAuth(username, password))


    # Check the status code
    if response.status_code == 200:
        # Print the response data
        return pd.DataFrame(response.json())
    else:
        # Print the error message
        print(f"Request failed: {response.status_code} {response.reason}")


def search_clients(username: str, password: str, base_url: str, branch_key: str, centre_key: str, offset=0, limit=100, client_type=None, mifos_id=None) -> pd.DataFrame:
    """
    param: base_url
    """

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.mambu.v2+json'
    }

    response = requests.post(
        url=f"{base_url}/clients:search",
        params={},
        data=json.dumps({
            "filterCriteria": [
                {
                    "field": "clientRoleKey",
                    "operator": "EQUALS",
                    "value": "8a19ce8e84db7c370184dd38dd916b29",
                }
            ],
            # "sortingCriteria": {
            #     "field": "encodedKey",
            #     "order": "ASC"
            # }
        }),
        headers=headers,
        auth=requests.auth.HTTPBasicAuth(username, password),
    )
    return response.json()


def get_client_roles(username: str, password: str, base_url: str) -> pd.DataFrame:
    response = requests.get(
        url=f"{base_url}/clienttypes",
        params={},
        auth=requests.auth.HTTPBasicAuth(username, password),
    )
    return response.json()


def get_clients(username: str, password: str, base_url: str, branch_key: str, centre_key: str, offset=0, limit=100, client_type=None, mifos_id=None) -> pd.DataFrame:
    """
    param: base_url
    """

    headers = {'Accept': 'application/vnd.mambu.v2+json', "clientType": client_type}
    # print(client_type)

    response = requests.get(
        url=f"{base_url}/clients" if client_type is None else f"{base_url}/clients?roles={client_type}",
        params={
            'offset': offset,
            'limit': limit,
            # 'branchId': branch_key,
            # 'centreId': centre_key,
            'paginationDetails': 'ON',
            'detailsLevel': 'FULL',
            # 'state': 'ACTIVE',
            'sortBy': 'creationDate:ASC',
            # "filterCriteria": [
            #     {
            #         "field": "_Additional_Information_Clients.investor_newsletter",
            #         "operator": "EQUALS_CASE_SENSITIVE",
            #         "value": "FALSE"
            #     }
            # ]
        },
        headers=headers,
        auth=requests.auth.HTTPBasicAuth(username, password),
    )
    # print(response.text)
    clients = response.json()
    for client in clients:
        print(client['firstName'])
        # print(client['firstName'])


    # Iterate over the clients and retrieve their custom fields
    # for client in clients:
    #     # Retrieve the custom fields of the client
    #     custom_fields_response = requests.get(
    #         f"{base_url}/clients/{client['id']}/custominformation",
    #         headers=headers,
    #         auth=requests.auth.HTTPBasicAuth(username, password),
    #     )
    #     custom_fields = custom_fields_response.json()
    #     print(client['id'], custom_fields)

    return pd.DataFrame(response.json())


def get_custom_fieldsets(base_url, username, password):
    # Set up the API endpoint URL
    custom_field_sets_endpoint = '/customfieldsets'
    url = base_url + custom_field_sets_endpoint

    # Set up the headers
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {access_token}'
    }

    # Send the GET request
    response = requests.get(url, headers=headers, auth=requests.auth.HTTPBasicAuth(username, password))

    # Get the JSON data from the response
    data = response.json()

    # Do something with the data, e.g. print it
    return data


def get_sandbox_settings(base_url, username, password, tenant_id):
    # Set the API endpoint URL
    api_url = f"{base_url}/tenant/{tenant_id}/settings"

    # Set the headers
    headers = {
        "Content-Type": "application/json",
    }

    # Make the GET request to export tenant settings
    response = requests.get(
        api_url,
        headers=headers,
        auth=requests.auth.HTTPBasicAuth(username, password),
    )

    # Print the response
    print(response.json())

    return response.json()


def get_custom_fields(base_url, username, password):
    # Set up the API endpoint URL
    custom_fields_endpoint = '/customfields'
    url = base_url + custom_fields_endpoint

    # Set up the headers
    headers = {
        'Content-Type': 'application/json',
    }

    # Send the GET request
    response = requests.get(url, headers=headers, auth=requests.auth.HTTPBasicAuth(username, password))

    # Get the JSON data from the response
    data = response.json()

    return data
    # Do something with the data, e.g. save it to a file
    # with open('custom_fields.json', 'w') as file:
    #     json.dump(data, file)


# if __name__=='__main__':
#     username = Variable.get('mambu_user_username')
#     password = Variable.get('mambu_user_password')
#     base_url = 'https://asante.sandbox.mambu.com/api'
#     tenant_id = 'asante'
#
#     client_roles = get_client_roles(
#         username=username,
#         password=password,
#         base_url=base_url
#     )
#     print([f"{role['name']} {role['encodedKey']}" for role in client_roles])
#
#     clients = search_clients(
#         base_url=base_url,
#         branch_key='KE',
#         # centre_key='007',
#         centre_key='',
#         offset=0,
#         limit=100,
#         username=username,
#         password=password,
#         client_type='Solv Client'
#
#     )
#     for client in clients:
#         print(client['firstName'])
    # clients2 = get_clients(
    #     base_url=base_url,
    #     branch_key='KE',
    #     # centre_key='007',
    #     centre_key='',
    #     offset=0,
    #     limit=100,
    #     username=username,
    #     password=password,
    #     # client_type='Solv Client'
    #
    # )
    # print(clients.columns)
    # loan_products = get_loan_products(
    #     base_url=base_url,
    #     offset=0,
    #     limit=100,
    #     username=username,
    #     password=password
    # )
    # loans = get_loans(
    #     base_url=base_url,
    #     offset=0,
    #     limit=100,
    #     branch_key='KE',
    #     centre_key='007',
    #     username=username,
    #     password=password
    # )
    # transactions = get_loans(
    #     base_url=base_url,
    #     offset=0,
    #     limit=100,
    #     branch_key='KE',
    #     centre_key='007',
    #     username=username,
    #     password=password
    # )
    # custom_fields = get_custom_fields(
    #     base_url=base_url,
    #     username=username,
    #     password=password
    # )
    # custom_fieldsets = get_custom_fieldsets(
    #     base_url=base_url,
    #     username=username,
    #     password=password
    # )
    # sandbox_settings = get_sandbox_settings(
    #     base_url=base_url,
    #     tenant_id=tenant_id,
    #     username=username,
    #     password=password
    # )

    # for fieldset in custom_fieldsets:
    #     for field in fieldset['customFields']:
    #         print(
    #             {
    #                 'name': field['name'],
    #                 'dataType': field['dataType'],
    #                 'valueLength': field['valueLength'],
    #                 'description': field['description'],
    #                 # 'values': field['values'],
    #             }
    #             , '-------------------------------------------------'
    #         )
    #     print('====================================================')
    # print([x['name'] for x in custom_fieldsets])


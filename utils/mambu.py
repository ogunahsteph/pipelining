import sys, os
import requests
import json
import pandas as pd
from airflow.models import Variable
import warnings

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import AsanteJson

AJSON = AsanteJson.JSON()
class MAMBU:
    def __init__(self):
        self.ClassName = type(self).__name__ + '.'
        warnings.simplefilter("ignore")
        self.username = Variable.get('mambu_user_username')
        self.password = Variable.get('mambu_user_password')
        self.base_url = Variable.get('mambu_api_server')
        self.tenant_id = 'asante'
        self.headers = {'Accept': 'application/vnd.mambu.v2+json'}
        self.offset = 0
        self.limit = 1000

    def data_columns_mapper(self, data : pd.DataFrame, mapping_data : pd.DataFrame) -> pd.DataFrame:
        """Maps the mambu columns to DWH columns"""

        for index, row in mapping_data.iterrows():
            source_name = row['Mambu']
            destination_name = row['DWH']


            # Rename column if it exists in data
            if source_name in data.columns:
                data.rename(columns={source_name: destination_name}, inplace=True)
            else:
                data[destination_name] = pd.NA

        return data


    def get_loan_products(self, offset=0, limit=100, product=None) -> pd.DataFrame:
        response = requests.get(
            f'{self.base_url}/loanproducts',
            params={
                'offset': offset,
                'limit': limit
            },
            headers=self.headers,
            auth=requests.auth.HTTPBasicAuth(self.username, self.password)
        )

        data = AJSON.json_to_dataframe(response.json())

        if product is not None:
            try:
                data = data[data['name'].str.lower().str.contains(str(product).lower(), na=False)]
            except:
                pass

        return data

    # def get_loan_products(self, product=None) -> pd.DataFrame:
    #     all_data = pd.DataFrame()
    #
    #     while True:
    #         response = requests.get(
    #             f'{self.base_url}/loanproducts',
    #             params={
    #             'offset': self.offset,
    #             'limit': self.limit
    #         },
    #             headers=self.headers,
    #             auth=requests.auth.HTTPBasicAuth(self.username, self.password)
    #         )
    #         print(response)
    #         data = AJSON.json_to_dataframe(response.json())
    #         print(data)
    #         if data.empty:
    #             break
    #
    #         if product is not None:
    #             data = data[data['name'].str.lower().str.contains(str(product).lower(), na=False)]
    #         all_data = pd.concat([all_data, data])
    #         self.offset += self.limit
    #         print(self.offset)
    #
    #     return all_data

    def get_loans(self, offset=0, limit=100) -> pd.DataFrame:
        url = f"{self.base_url}/loans"

        # request parameters (optional)
        params = {
            "offset": offset,
            "limit": limit,
            "fullDetails": False,
        }

        response = requests.get(url, headers=self.headers, params=params,
                                auth=requests.auth.HTTPBasicAuth(self.username, self.password))

        data = AJSON.json_to_dataframe(response.json())

        return data

    def get_clients(self, offset=0, limit=1000, client_type=None, mifos_id=None) -> pd.DataFrame:
        response = requests.get(
            url=f"{self.base_url}/clients" if client_type is None else f"{self.base_url}/clients?roles={client_type}",
            params={
                'offset': offset,
                'limit': limit,
                'paginationDetails': 'OFF',
                'detailsLevel': 'FULL',
                # 'state': 'ACTIVE',
                # 'sortBy': 'creationDate:ASC',
            },
            headers=self.headers,
            auth=requests.auth.HTTPBasicAuth(self.username, self.password),
        )

        data = AJSON.json_to_dataframe(response.json())

        return data

    def get_transactions_by_id(self, offset=0, limit=1000, loan_id=None) -> pd.DataFrame:
        """returns the transactions by id"""
        url = f"{self.base_url}/loans/{str(loan_id)}/transactions"

        params = {
            'offset': offset,
            'limit': limit,
            'paginationDetails': 'OFF'
        }

        # Make the request
        response = requests.get(url, headers=self.headers,
                                auth=requests.auth.HTTPBasicAuth(self.username, self.password),
                                params=params)
        data = AJSON.json_to_dataframe(response.json())

        return data

    def search_transactions(self, offset=0, limit=1000, centre_keys=None) -> pd.DataFrame:
        """returns the transactions by search"""
        url = f"{self.base_url}/loans/transactions:search"

        params = {
            'offset': offset,
            'limit': limit,
            'paginationDetails': 'OFF'
        }

        body = {
            "filterCriteria": [
                {
                    "field": "centreKey",
                    "operator": "IN",
                    "values": centre_keys
                }
            ],
            "sortingCriteria": {
                "field": "id",
                "order": "ASC"
            }
        }

        # Make the request
        response = requests.post(url, headers=self.headers,
                                 auth=requests.auth.HTTPBasicAuth(self.username, self.password),
                                 params=params, json=body)

        data = AJSON.json_to_dataframe(response.json())

        return data

    def get_partners(self, filter=None, limit=1000, offset=0):
        """Centres on Mambu"""
        params = {
            'offset': offset,
            'limit': limit,
            'paginationDetails': 'OFF'
        }
        mambu_url = f"{self.base_url}/centres"
        response = requests.get(mambu_url, headers=self.headers, params=params,
                                auth=requests.auth.HTTPBasicAuth(self.username, self.password))

        data = AJSON.json_to_dataframe(response.json())

        if filter is not None:
            try:
                data = data[data['name'].str.lower().str.contains(str(filter).lower(), na=False)]
            except:
                pass


        return data


if __name__ == '__main__':
    MAMBU = MAMBU()
    # MAMBU.get_loan_products(product='mtn')
    # MAMBU.get_loans()
    # MAMBU.get_clients()
    MAMBU.get_transactions_by_id(1)

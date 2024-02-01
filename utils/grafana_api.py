import json
import requests
from airflow.models import Variable


def get_url(host: str):
    if host == 'AWS':
        return 'https://grafana.asantefsg.com/api'
    elif host == 'DO':
        return 'https://192.241.150.164/dashboard/api'


class GrafanaAPI:
    def __init__(self):
        self.grafana_api_key_aws = Variable.get('grafana_api_key_aws')
        self.grafana_api_key_digital_ocean = Variable.get('grafana_api_key_digital_ocean')

    def get_dashboard(self, host: str, uid: str):
        base_url = get_url(host=host)
        res = requests.get(
            url=f'{base_url}/dashboards/uid/{uid}',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f"Bearer { self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws }"
            },
            verify=False
        )
        return json.loads(res.content)

    def get_data_sources(self, host:str):
        base_url = get_url(host)
        res = requests.get(
            url=f"{base_url}/datasources",
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws}"
            },
            verify = False
        )
        return json.loads(res.text)

    def get_team(self, host: str, team_name: str):
        base_url = get_url(host)
        res = requests.get(
            url=f"{base_url}/teams/search?name={team_name}",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws}"
            },
            verify=False
        )
        return json.loads(res.text)

    def get_folders(self, host: str):
        base_url = get_url(host)
        res = requests.get(
            url=f"{base_url}/folders",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws}"
            },
            verify=False
        )
        return json.loads(res.text)

    def create_team(self, payload: dict, host: str):
        base_url = get_url(host=host)

        res = requests.post(
            url=f'{base_url}/teams',
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer { self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws }"
            },
            data=json.dumps(payload),
            verify=False
        )
        return json.loads(res.content)

    def create_dashboard(self, payload: dict, host: str):
        base_url = get_url(host=host)

        res = requests.post(
            url=f'{base_url}/dashboards/db',
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer { self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws }"
            },
            data=json.dumps(payload),
            verify=False
        )
        return json.loads(res.content)

    def create_data_source(self, payload: dict, host: str):
        base_url = get_url(host)
        res = requests.post(
            url=f"{base_url}/datasources",
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws}"
            },
            data=json.dumps(payload),
            verify = False
        )
        return json.loads(res.text)

    def create_folder(self, payload: dict, host: str):
        base_url = get_url(host)
        res = requests.post(
            url=f"{base_url}/folders",
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.grafana_api_key_digital_ocean if host == 'DO' else self.grafana_api_key_aws}"
            },
            data=json.dumps(payload),
            verify = False
        )
        return json.loads(res.text)

import pandas as pd
import json
import requests
import warnings
from copy import deepcopy

warnings.simplefilter("ignore")


class JSON:
    def __init__(self):
        self.class_name = type(self).__name__

    def json_to_dataframe(self, json_data):
        def cross_join(left, right):
            new_rows = [] if right else left
            for left_row in left:
                for right_row in right:
                    temp_row = deepcopy(left_row)
                    for key, value in right_row.items():
                        temp_row[key] = value
                    new_rows.append(deepcopy(temp_row))
            return new_rows

        def flatten_list(data):
            for elem in data:
                if isinstance(elem, list):
                    yield from flatten_list(elem)
                else:
                    yield elem

        def js_to_dataframe(data_in):
            def flatten_json(data, prev_heading=''):
                if isinstance(data, dict):
                    rows = [{}]
                    for key, value in data.items():
                        rows = cross_join(rows, flatten_json(value, prev_heading + ' ' + key))
                elif isinstance(data, list):
                    rows = []
                    for i in range(len(data)):
                        [rows.append(elem) for elem in flatten_list(flatten_json(data[i], prev_heading))]
                else:
                    rows = [{prev_heading[1:]: data}]
                return rows

            return pd.DataFrame(flatten_json(data_in))

        if isinstance(json_data, str):
            response = requests.get(json_data)
            if response.status_code == 200:
                json_data = response.json()
            else:
                print(f'>> Failed...! The Json API Response Status : {response.status_code}')
                sys.exit()
        else:
            json_data = json_data

        DataFrame = js_to_dataframe(json_data)

        return DataFrame


if __name__ == '__main__':
    js = JSON()
    # js.json_to_dataframe()

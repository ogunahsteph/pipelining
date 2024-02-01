import warnings
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import sys

class WAREHOUSE:
    def __init__(self):
        self.ClassName = type(self).__name__ + '.'
        warnings.simplefilter("ignore")
        self.warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                           schema='afsg_ds_prod_postgresql_dwh')

    def get_partners(self, table_name, columns):
        warehouse_data = self.warehouse_hook.get_pandas_df(
            f"""SELECT * FROM {table_name}""")
        warehouse_data = warehouse_data.replace({'None': pd.NA})
        warehouse_data = warehouse_data.replace({None: pd.NA})
        warehouse_data = warehouse_data[columns]

        return warehouse_data

    def format_tuple(self, t):
        if len(t) == 1:
            # remove the comma for tuple with one element
            return f' = \'{t[0]}\''
        else:
            # Tuple with more than one element: convert to string as is
            return f' in {str(t)}'

    def get_warehouse_data(self, table_name: str, columns: list, unique_key_ids: list, unique_column: str, full_data = False):
        if not unique_key_ids:
            pass

        query = f"SELECT {'*' if full_data else unique_column} FROM {table_name} WHERE {unique_column} IN %(unique_key_ids)s"
        warehouse_data = self.warehouse_hook.get_pandas_df(
            query, parameters={'unique_key_ids': tuple(unique_key_ids)}
        )

        warehouse_data = warehouse_data.replace({None: pd.NA, 'None': pd.NA})

        if full_data:
            warehouse_data = warehouse_data[columns]

        return warehouse_data

    def escape_quotes_on_sql(self, value):
        if isinstance(value, str):
            return value.replace("'", "''")
        return value

    def insert_new_records_to_dwh(self, data, warehouse_db, columns):
        if 'encoded_key' in data.columns:
            warehouse_data = self.get_warehouse_data(
                table_name=warehouse_db,
                columns=columns,
                unique_column='encoded_key',
                unique_key_ids=data['encoded_key'].tolist()
            )
            new_data = data[~data['encoded_key'].str.strip().isin(warehouse_data['encoded_key'].str.strip())]
        elif 'kyc_msisdn_random' in data.columns:
            warehouse_data = self.get_warehouse_data(
                table_name=warehouse_db,
                columns=columns,
                unique_column='kyc_msisdn_random',
                unique_key_ids=data['kyc_msisdn_random'].tolist()
            )
            new_data = data[~data['kyc_msisdn_random'].str.strip().isin(warehouse_data['kyc_msisdn_random'].str.strip())]
            # new_data = data
        else:
            sys.exit("Required columns are not present in both dataframes.")

        if not new_data.empty:
            self.warehouse_hook.insert_rows(
                table=warehouse_db,
                target_fields=columns,
                rows=new_data[columns].where(pd.notna(new_data), None).itertuples(index=False, name=None),
                commit_every=100
            )

        return True

    def update_dwh_data(self, warehouse_db, mambu_data, data_columns):
        if 'encoded_key' in mambu_data.columns:
            warehouse_data = self.get_warehouse_data(
                table_name=warehouse_db,
                columns=data_columns,
                unique_column='encoded_key',
                unique_key_ids=mambu_data['encoded_key'].tolist(),
                full_data=True
            )
        elif 'kyc_msisdn_random' in mambu_data.columns:
            warehouse_data = self.get_warehouse_data(
                table_name=warehouse_db,
                columns=data_columns,
                unique_column='kyc_msisdn_random',
                unique_key_ids=mambu_data['kyc_msisdn_random'].tolist(),
                full_data=True
            )

        # Merge the old and new DataFrames using a left join
        merged_df = mambu_data.merge(warehouse_data, how='outer', indicator=True)

        # Filtering out rows that differ in the new DataFrame
        new_or_diff_rows_df = merged_df[merged_df['_merge'] == 'left_only']

        # Dropping the merge indicator column
        updated_records = new_or_diff_rows_df.drop(columns=['_merge'])
        updated_records = updated_records.where(pd.notna(updated_records), None)

        if 'encoded_key' in mambu_data.columns:
            updates = [
                f"UPDATE {warehouse_db} SET " + ", ".join(
                    f"{col} = NULL" if row[col] is None
                    else f"{col} = FALSE" if row[col] is False
                    else f"{col} = True" if row[col] is True
                    else f"{col} = '{self.escape_quotes_on_sql(row[col])}'" for col in data_columns if col != 'encoded_key'
                ) + f" WHERE encoded_key = '{row['encoded_key']}'" for index, row in updated_records.iterrows()
            ]

        elif 'kyc_msisdn_random' in mambu_data.columns:
            updates = [
                f"UPDATE {warehouse_db} SET " + ", ".join(
                    f"{col} = NULL" if row[col] is None
                    else f"{col} = FALSE" if row[col] is False
                    else f"{col} = True" if row[col] is True
                    else f"{col} = '{self.escape_quotes_on_sql(row[col])}'" for col in data_columns if col != 'kyc_msisdn_random'
                ) + f" WHERE kyc_msisdn_random = '{row['kyc_msisdn_random']}'" for index, row in updated_records.iterrows()
            ]

        if len(updates) > 0:
            self.warehouse_hook.run(sql=updates)


if __name__ == '__main__':
    WAREHOUSE = WAREHOUSE()
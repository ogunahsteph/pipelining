import io
import boto3
import pandas as pd
from airflow.models import Variable

def save_file_to_s3(file_bytes: str, bucket_name: str, s3_file_key: str) -> None:
    """
     Save file bytes as an object in Amazon S3.

    :param file_bytes: The file bytes to be saved as an object in S3.
    :type file_bytes: str

    :param bucket_name: The name of the S3 bucket where the object will be stored.
    :type bucket_name: str

    :param s3_file_key: The key or path under which the object will be saved in the S3 bucket.
    :type s3_file_key: str

    :return: None
    """

    # Connect to S3 using access key and secret key
    s3 = boto3.client(
        "s3",
        aws_access_key_id=Variable.get('AFSG_aws_access_key_id') if Variable.get('DEBUG') == 'FALSE' else None,
        aws_secret_access_key=Variable.get('AFSG_aws_secret_access_key') if Variable.get('DEBUG') == 'FALSE' else None
    )

    # Upload the file bytes to S3 as an object
    s3.put_object(Body=file_bytes, Bucket=bucket_name, Key=s3_file_key)

def get_objects(bucket_name: str, search: str, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Retrieve objects from Amazon S3 that match a given search string in their names and return them as a list of DataFrames.

    :param bucket_name: The name of the S3 bucket to search for objects.
    :type bucket_name: str

    :param aws_access_key_id: The AWS access key ID to authenticate with Amazon S3.
    :type aws_access_key_id: str

    :param aws_secret_access_key: The AWS secret access key to authenticate with Amazon S3.
    :type aws_secret_access_key: str

    :param search: The search string to filter objects in the S3 bucket.
    :type search: str

    :return: A list of dictionaries containing file names and corresponding DataFrames for objects found in S3.
    :rtype: list
    """

    # Connect to S3 using access key and secret key
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    files = []

    # Initialize continuation token as None
    continuation_token = None

    # Cater for pagination / Iterate all files in bucket
    while True:
        # Make the list_objects_v2 API call with the continuation token
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name)

        # Iterate over the objects and fetch files that contain the search in their names
        if 'Contents' in response:
            for obj in response['Contents']:
                file_name = obj['Key']

                if search.lower() in file_name.lower():
                    # Determine the file format based on the file extension
                    file_extension = file_name.split('.')[-1].lower()

                    # Read the file directly into a pandas DataFrame
                    obj_data = s3.get_object(Bucket=bucket_name, Key=file_name)

                    if file_extension == 'csv':
                        df = pd.read_csv(obj_data['Body'])
                    elif file_extension in ['xls', 'xlsx']:
                        df = pd.read_excel(io.BytesIO(obj_data['Body'].read()))
                    else:
                        raise ValueError(f"Unsupported file format: {file_extension}")

                    # Append the DataFrame to the list
                    files.append({'name': file_name, 'data': df})

        # Check if there are more objects to fetch
        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return files


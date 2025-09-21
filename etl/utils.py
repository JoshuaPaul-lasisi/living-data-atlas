import os
import json
import boto3
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

load_dotenv() # load env variables

def save_data(data, path, with_timestamp=True):
    """
    Save the data to either the local file system or S3 
    based on the path. the reason for this is that AWS 
    verification is delaying so I want to keep working 
    locally pending when it works.
    
    Appending a  timestamp so that they don't overwrite themselves. I don't want problem.

    Args:
        data (dict or list): the data we are saving
        path (str): one of either local file path or S3 URI
    """
    if with_timestamp and not path.startswith('s3://'):
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        root, ext = os.path.splitext(path)
        path = f'{root}_{timestamp}{ext}'
    
    if path.startswith('s3://'):
        # we will start by considering cloud first before local
        bucket_name = path.split('/')[2] # s3://bucket-name/path/to/file is the standard AWS URI format
        key = '/'.join(path.split('/')[3:]) # the rest is the key
        
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_REGION')
            )
            s3.put_object( #upload to s3
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data)
            )
            print(f'Saved to S3: {path}') # I'll replace it with logging 
        except (NoCredentialsError, ClientError) as e:
            print(f'Failed to save to S3, error: {e}')
    else:
        # now for local
        Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f: # just for proper handling
            json.dump(data, f)
        print(f'Saved locally: {path}')
        
        # I'll replace with logging when deploying

def fetch_api_data(api_url, params=None):
    """
    Fetch data from a REST API endpoint and return the JSON response.

    Args:
        api_url (str): The API endpoint URL.
        params (dict, optional): Query parameters for the API request.
    """
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()
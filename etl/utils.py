import os
import json
import boto3
from pathlib import Path


def save_data(data, path):
    """
    Save the data to either the local file system or S3 
    based on the path. the reason for this is that AWS 
    verification is delaying so I want to keep working 
    locally pending when it works.

    Args:
        data (dict or list): the data we are saving
        path (str): one of either local file path or S3 URI
    """
    if path.startswith('s3://'):
        # we will start by considering cloud first before local
        bucket_name = path.split('/')[2] # s3://bucket-name/path/to/file is the standard AWS URI format
        key = '/'.join(path.split('/')[3:]) # the rest is the key
        
        s3 = boto3.client('s3')
        s3.put_object( #upload to s3
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data)
        )
        print(f'Saved to S3: {path}') # I'll replace it with logging later
    else:
        # now for local
        Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f: # just for proper handling
            json.dump(data, f)
        print(f'Saved locally: {path}')
        
        # I'll replace with logging when deploying
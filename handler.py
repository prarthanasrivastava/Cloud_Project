
import os
import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        try:
            # Get object metadata
            head = s3.head_object(Bucket=bucket, Key=key)
            retention_until = head['Metadata'].get('retention_until') # format: YYYY-MM-DD
            if not retention_until:
                print(f'No retention metadata for {key}, skipping.')
                continue
            retention_date = datetime.strptime(retention_until, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if now >= retention_date:
                print(f'Deleting expired object: {key}')
                s3.delete_object(Bucket=bucket, Key=key)
            else:
                print(f'Object {key} still in retention period.')
        except Exception as e:
            print(f'Error checking or deleting {key}: {e}')
    return {"status": "completed"}

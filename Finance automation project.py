# lambda_function.py

import json
import boto3
import os

s3      = boto3.client('s3')
athena  = boto3.client('athena')

# Environment variables set in Lambda console:
#   DATA_BUCKET, ATHENA_DB, ATHENA_TABLE, ATHENA_OUTPUT

def lambda_handler(event, context):
    # 1. Read raw JSON from S3
    rec   = event['Records'][0]['s3']
    bucket, key = rec['bucket']['name'], rec['object']['key']
    obj   = s3.get_object(Bucket=bucket, Key=key)
    data  = json.load(obj['Body'])

    # 2. Extract only key fields
    out = {
        'user_id': data.get('userId'),
        'event_ts': data.get('metadata', {}).get('timestamp'),
        'metric': data.get('payload', {}).get('metricValue')
    }
    csv_body = f"{out['user_id']},{out['event_ts']},{out['metric']}"

    # 3. Write processed CSV back to S3
    proc_key = key.replace('raw/', 'processed/').replace('.json', '.csv')
    s3.put_object(Bucket=bucket, Key=proc_key, Body=csv_body)

    # 4. Refresh Athena partitions
    athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {os.environ['ATHENA_TABLE']};",
        QueryExecutionContext={'Database': os.environ['ATHENA_DB']},
        ResultConfiguration={'OutputLocation': os.environ['ATHENA_OUTPUT']}
    )
    return {'status': 'success', 'processed_key': proc_key}

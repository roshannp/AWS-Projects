# lambda/etl/etl.py

import os
import json
import boto3
import pandas as pd

s3 = boto3.client('s3')
RAW = os.environ['RAW_BUCKET']
PROC = os.environ['PROC_BUCKET']

def handler(event, context):
    # Process each new raw file
    for rec in event['Records']:
        key = rec['s3']['object']['key']
        body = s3.get_object(Bucket=RAW, Key=key)['Body']
        df   = pd.read_json(body)

        # Aggregate by region and hour to anonymize
        agg = (df
               .groupby([df.region, df.view_timestamp.dt.floor('H')])
               .view_count
               .sum()
               .reset_index())

        out_key = key.replace("raw/", "aggregated/").replace(".json", ".csv")
        s3.put_object(
            Bucket=PROC,
            Key=out_key,
            Body=agg.to_csv(index=False)
        )

    return {"status": "aggregated"}

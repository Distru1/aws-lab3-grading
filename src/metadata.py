import json
import os
import boto3
from PIL import Image
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        bucket = body['bucket']
        key = body['key']
        
        filename = os.path.basename(key)
        metadata_key = f"metadata/{filename}.json"
        
        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            continue
        except:
            pass
            
        obj = s3.get_object(Bucket=bucket, Key=key)
        img_data = obj['Body'].read()
        
        with Image.open(BytesIO(img_data)) as img:
            metadata = {
                "source_bucket": bucket,
                "source_key": key,
                "width": img.width,
                "height": img.height,
                "file_size_bytes": obj['ContentLength'],
                "format": img.format
            }
            
        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType='application/json'
        )
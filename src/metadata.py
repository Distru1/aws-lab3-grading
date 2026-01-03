import json
import boto3
import io
from PIL import Image

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        bucket = body['bucket']
        key = body['key']
        
        # Idempotency check: Does metadata already exist?
        metadata_key = f"metadata/{key.split('/')[-1]}.json"
        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            print(f"Metadata already exists for {key}. Skipping.")
            continue
        except:
            pass # Doesn't exist, proceed

        # Download and extract
        obj = s3.get_object(Bucket=bucket, Key=key)
        img = Image.open(io.BytesIO(obj['Body'].read()))
        
        metadata = {
            "format": img.format,
            "width": img.width,
            "height": img.height,
            "size_bytes": obj['ContentLength']
        }

        # Save to metadata/ prefix
        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType="application/json"
        )
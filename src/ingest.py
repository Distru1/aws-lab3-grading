import os
import json
import boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    etag = record['s3']['object']['eTag']

    # Validate extension
    if not key.lower().endswith(('.png', '.jpg', '.jpeg')):
        return {"status": "skipped", "reason": "not an image"}

    # Send clean message to SQS
    message = {
        "bucket": bucket,
        "key": key,
        "etag": etag
    }
    
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    return {"status": "ingested", "key": key}
import boto3
import urllib.parse
from datetime import datetime

stepfunctions = boto3.client('stepfunctions')

# Replacing with our actual Step Function ARN
STATE_MACHINE_ARN = "arn:aws:states:ap-southeast-2:123456789012:stateMachine:ETL-Medallion-Pipeline"

def lambda_handler(event, context):
    print("Received event:", event)
    
    # Extracting bucket and key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    if not key.endswith(".csv"):
        print(f"Skipped non-CSV file: {key}")
        return

    # Extracting date partitions from key
    try:
        parts = key.split('/')
        year = int(parts[1])
        month = int(parts[2])
        day = int(parts[3])
    except (IndexError, ValueError) as e:
        print(f"Invalid S3 key structure: {key}")
        raise e

    # Constructing full S3 path
    s3_path = f"s3://{bucket}/{key}"

    # Input payload for Step Function execution
    input_payload = {
        "year": year,
        "month": month,
        "day": day,
        "s3_path": s3_path
    }

    # Starting step function
    response = stepfunctions.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(input_payload)
    )

    print(f"Started Step Function execution: {response['executionArn']}")
    return {
        'statusCode': 200,
        'body': f"Triggered Step Function for file: {key}"
    }
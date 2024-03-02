# create presigned url
import json
import boto3
import urllib

session = boto3.session.Session(region_name='us-west-2')
s3 = session.client('s3', config=boto3.session.Config(signature_version='s3v4'))

def create_presigned_url(bucket, key):
    
    presigned_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=500
    )
    print("pre-signed = ", presigned_url)
    return presigned_url

# img = 's3://epic-bolster-images/images/crashed.png'
def lambda_handler(event, context):
    print("lambda event ",event)
    bucket = event['Bucket']
    key = event['Key']
    presigned_url = create_presigned_url(bucket, key)
    s3_presigned={"presigned_url":presigned_url}
    s3_presigned_url=json.dumps(s3_presigned)
    return presigned_url

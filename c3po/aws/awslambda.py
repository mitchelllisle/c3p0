import boto3
import json


def invokeLambda(accessKey, secret, arn, payload, region='ap-southeast-2'):
    try:
        client = boto3.client('lambda', aws_access_key_id=accessKey, aws_secret_access_key=secret, region_name=region)
        res = client.invoke(FunctionName=arn, Payload=json.dumps(payload), InvocationType='Event')
        return res
    except Exception as e:
        return str(e)

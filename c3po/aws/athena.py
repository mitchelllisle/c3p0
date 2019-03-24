import boto3
from .s3 import extractBucketName
from .s3 import fetchS3


def getExecutionStatus(executionId, client):
    execution = client.get_query_execution(QueryExecutionId=executionId)
    outputLocation = execution['QueryExecution']['ResultConfiguration']['OutputLocation']
    status = execution['QueryExecution']['Status']['State']
    return status, outputLocation


def queryAthena(access_key, access_secret, query, resultLocation):
    client = boto3.client('athena', aws_access_key_id=access_key, aws_secret_access_key=access_secret)
    queryRequest = client.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': resultLocation})

    executionStatus = getExecutionStatus(str(queryRequest['QueryExecutionId']), client)
    status = executionStatus[0]

    while status == 'RUNNING':
        executionStatus = getExecutionStatus(str(queryRequest['QueryExecutionId']), client)
        status = executionStatus[0]

    resultDataLocation = extractBucketName(executionStatus[1])
    resultData = fetchS3(access_key, access_secret, resultDataLocation[0], resultDataLocation[1][0])
    return resultData

import pandas as pd
import boto3
import io
from io import StringIO
import json

def fetchS3(access_key, secret, bucket, file, returnObj = False, naFilter = True):
    '''
    Fetch a file from an AWS S3 Bucket
    -----------
    DETAILS
    -----------
    This function is used to read files from s3 buckets. Currently it will only work with
    csv files
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    file : The file within the specified bucket to download. This should also include the
    relative path. Preceeding forward slash can be ommitted. E.g. 'myfolder/myfile.csv'
    returnObj : This will return the object instance of the file rather than reading the file into a dataframe. Useful
    in instances where you don't want to parse the file as a pandas dataframe.
    '''
    s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
    obj = s3.get_object(Bucket = bucket, Key = file)

    if returnObj == False:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), na_filter = naFilter)
        return df
    else:
        return obj

def putS3(access_key, secret, bucket, file, data, includeIndex = True):
    '''
    Put a file on to an AWS S3 Bucket
    -----------
    DETAILS
    -----------
    This function is used to put files on S3. Currently it will only work with
    csv files
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    file : The file within the specified bucket to download. This should also include the
    relative path. Preceeding forward slash can be ommitted. E.g. 'myfolder/myfile.csv'
    data : The data object to save. Only rested with Pandas DataFrames currently.
    '''
    s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)

    csv_buffer = StringIO()

    data.to_csv(csv_buffer, index = includeIndex)

    obj = s3.put_object(Bucket = bucket, Key = file, Body = csv_buffer.getvalue())

    if obj["ResponseMetadata"]["HTTPStatusCode"] == 200:
        output = "s3://" + bucket + "/" + file
    else:
        output = obj["ResponseMetadata"]["HTTPStatusCode"]

    return output

def listFiles(access_key, secret, bucket, folder = '', startAfter = ''):
    '''
    List all files in a bucker or folder on S3
    -----------
    DETAILS
    -----------
    This function is used to list files in an S3 Bucket or within a folder on S3
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    folder : The folder structure (e.g. myFolder/mySubFolder)
    startAfter : Load objects that appear after this key only
    '''
    s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
    response = s3.list_objects_v2(Bucket = bucket, Prefix = folder, StartAfter = startAfter)
    allKeys = []
    for i in range(0, len(response['Contents'])):
        allKeys.append(response['Contents'][i]['Key'])
    allKeys = list(filter(lambda x: str(x).endswith(".csv"), allKeys))
    return allKeys


def extractBucketName(location):
    '''
    Extract bucket name and file from s3:// URI
    '''
    params = location.split("//")[1]
    bucket = params.split("/")[0]
    path = params.split("/", maxsplit = 1)[1:]
    return bucket, path


def invokeLambda(accessKey, secret, arn, payload, region = 'ap-southeast-2'):
    try:
        client = boto3.client('lambda', aws_access_key_id = accessKey, aws_secret_access_key = secret, region_name = region)
        res = client.invoke(FunctionName = arn, Payload = json.dumps(payload), InvocationType = 'Event')
        return res
    except Exception as e:
        return str(e)

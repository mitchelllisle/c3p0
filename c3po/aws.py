import boto3
import datetime as dt
import json
from .aws_utils import body_parsers
from .aws_utils import filter_files
from .aws_utils import convert_data_for_upload


class S3:
    def __init__(self, access_key, access_secret, region="ap-southeast-2"):
        self.access_key = access_key
        self.access_secret = access_secret
        self.region = region

        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.access_secret,
            region_name=self.region
        )

    def fetchS3(self, bucket, filename, parse_body=True, na_filter=True):
        object = self.client.get_object(
            Bucket=bucket,
            Key=filename
        )

        if parse_body is True:
            parsed_object = body_parsers(
                object=object,
                filename=filename,
                na_filter=na_filter
            )
            return parsed_object
        else:
            return object

    def putS3(self, bucket, filename, data):
        self.bucket = bucket
        self.filename = filename
        self.data = data
        self.data_type = type(data)

        res = self.client.put_object(
            Bucket=self.bucket,
            Key=self.filename,
            Body=convert_data_for_upload(
                data=self.data,
                type=self.data_type
            )
        )

        if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return "s3://{}/{}".format(bucket, filename)
        else:
            raise Exception("putS3 returned error code {}".format(res["ResponseMetadata"]["HTTPStatusCode"]))

    def listFiles(self, bucket, endswith=None, folder='', start_after=''):
        response = self.client.list_objects_v2(
            Bucket=bucket,
            Prefix=folder,
            StartAfter=start_after
        )

        all_keys = []
        for i in range(0, len(response['Contents'])):
            all_keys.append(response['Contents'][i]['Key'])

        all_keys = filter_files(all_keys, endswith)
        return all_keys


class ElasticMapReduce:
    def __init__(self, access_key, access_secret, region="ap-southeast-2"):
        self.timestamp = dt.datetime.now()

        self.client = boto3.client(
            'emr',
            aws_access_key_id=access_key,
            aws_secret_access_key=access_secret,
            region_name=region
        )

    def list_clusters(self, createdAfter=None, createdBefore=None, **kwargs):
        """
        ClusterStates: 'STARTING' ,'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'
        Marker: Where to list the results from.
        """

        if createdAfter is None:
            createdAfter = (dt.datetime.today() - dt.timedelta(30))

        if createdBefore is None:
            createdBefore = dt.datetime.today()

        response = self.client.list_clusters(
            CreatedAfter=createdAfter,
            CreatedBefore=createdBefore,
            **kwargs
        )

        return response

    def create_and_run_workflow(self):
        "hadoop-streaming -files {mapper},{reducer} -mapper mapper.py -reducer reducer.py -input {input} -output {output}".format(
            mapper="s3://mlisle-data/mapper.py",
            reducer="s3://mlisle-data/reducer.py",
            input="s3://mlisle-data/data/",
            output="s3://mlisle-data/emr_results/"
        )

        response = self.client.run_job_flow(
            Name="Boto3 test cluster",
            ReleaseLabel='emr-5.12.0',
            Instances={
                'MasterInstanceType': 'm4.large',
                'SlaveInstanceType': 'm4.large',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        return response


class Lambda:
    def __init__(self, arn, access_key, access_secret, region='ap-southeast-2', invocation_type="Event"):
        self.access_key = access_key
        self.access_secret = access_secret
        self.arn = arn
        self.invocation_type = invocation_type

        self.client = boto3.client(
            "lambda",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.access_secret,
            region_name=self.region
        )

    def invoke(self, payload: dict) -> dict:
        result = self.client.invoke(
            FunctionName=self.arn,
            Payload=json.dumps(payload),
            InvocationType=self.invocation_type
        )
        return result

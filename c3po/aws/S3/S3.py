import boto3
from .S3_utils import body_parsers
from .S3_utils import filter_files


class S3:
    def __init__(self, access_key, access_secret, region):
        self.access_key = access_key
        self.access_secret = access_secret

        self.client = boto3.client(
            "s3",
            self.access_key,
            self.access_secret,
            self.region
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

    def putS3(self, bucket, filename, data, includeIndex=True):
        self.filename
        self.data

        res = self.client.put_object(
            Bucket=self.bucket,
            Key=self.filename,
            Body=self.data
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

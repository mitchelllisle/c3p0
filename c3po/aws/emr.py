import boto3
import datetime as dt


class ElasticMapReduce:
    def __init__(self, accessKey, accessSecret, region="ap-southeast-2"):
        self.timestamp = dt.datetime.now()

        self.client = boto3.client(
            'emr',
            aws_access_key_id=accessKey,
            aws_secret_access_key=accessSecret,
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

emr = ElasticMapReduce(accessKey, accessSecret)

emr.list_clusters()

emr.create_and_run_workflow()

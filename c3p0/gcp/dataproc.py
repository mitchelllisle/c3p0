from googleapiclient.discovery import build


def get_client(gcs_creds):
    """
    Builds a client to the dataproc API.
    """
    dataproc = build('dataproc', 'v1', credentials=gcs_creds)
    return dataproc


def list_clusters_with_details(dataproc, project, region):
    result = dataproc.projects().regions().clusters().list(
        projectId=project,
        region=region).execute()
    cluster_list = result.get("clusters")

    if cluster_list is not None:
        for cluster in cluster_list:
            print("{} - {}"
                  .format(cluster['clusterName'], cluster['status']['state']))
    return result


def delete_cluster(dataproc, project, region, cluster):
    print('Tearing down cluster')
    result = dataproc.projects().regions().clusters().delete(
        projectId=project,
        region=region,
        clusterName=cluster).execute()
    return result


def create_cluster(dataproc, project, zone, region, cluster_name):
    print('Creating cluster...')
    zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(project, zone)

    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'gceClusterConfig': {
                'zoneUri': zone_uri
            },
            'masterConfig': {
                'numInstances': 1,
                'machineTypeUri': 'n1-standard-1'
            },
            'workerConfig': {
                'numInstances': 2,
                'machineTypeUri': 'n1-standard-1'
            }
        }
    }
    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()
    return result


def submit_pyspark_job(dataproc, project, region, cluster_name, bucket_name, filename):
    """Submits the Pyspark job to the cluster, assuming `filename` has
    already been uploaded to `bucket_name`"""
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://{}/{}'.format(bucket_name, filename)
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    return job_id


def wait_for_job(dataproc, project, region, job_id):
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project,
            region=region,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished.')
            return result

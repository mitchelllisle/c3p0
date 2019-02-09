from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from io import StringIO
import os


def gcsAuth(keyfile):
    if keyfile is not None:
        gcs_credentials = service_account.Credentials.from_service_account_file(os.path.expanduser(keyfile))
    else:
        gcs_credentials = None
    return gcs_credentials


def putGCS(credentials, data, project, bucket, file_name, includeIndex=False):
    """
    Upload a file to Google Storage.
    This function will upload a Pandas CSV DataFrame to a google storage bucket.
    Args:
    credentials: The location of your keyfile.json
    data: The dataframe you want to save
    project: Google project identifier
    bucket: The name of the bucket in the project
    file_name: The path and file name to save
    includeIndex: Include the index as part of writing the file to GCS
    """
    gcs_credentials = gcsAuth(credentials)

    client = storage.Client(project=project, credentials=gcs_credentials)
    bucket = client.get_bucket(bucket)

    blob = bucket.blob(file_name)
    # TODO: Something like this for JSON. Have to deal with NaNs/None though
    # import json
    # blob.upload_from_string(json.dumps(data.to_dict()))
    blob.upload_from_string(data.to_csv(index=includeIndex), content_type="text/csv")

    return {"project": project, "bucket": bucket, "file": file_name}


def convertContentType(blob):
    blob_type = blob.content_type
    s = str(blob.download_as_string(), 'utf-8')
    data = StringIO(s)
    if blob_type == 'text/csv':
        blob_content = pd.read_csv(data)
    elif blob_type == 'text/json':
        blob_content = pd.read_json(data)
    return blob_content


def fetchGCS(credentials, project, bucket, file_name):
    """
    Upload a file to Google Storage.
    This function will upload a Pandas CSV DataFrame to a google storage bucket.
    Args:
    credentials: The location of your keyfile.json
    data: The dataframe you want to save
    project: Google project identifier
    bucket: The name of the bucket in the project
    file_name: The path and file name to save
    includeIndex: Include the index as part of writing the file to GCS
    """
    gcs_credentials = gcsAuth(credentials)

    client = storage.Client(project=project, credentials=gcs_credentials)
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_name)

    data = convertContentType(blob)
    return data

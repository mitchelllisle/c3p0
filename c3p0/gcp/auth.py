from google.oauth2 import service_account
import os


def gcsAuth(keyfile):
    if keyfile is not None:
        gcs_credentials = service_account.Credentials.from_service_account_file(os.path.expanduser(keyfile))
    else:
        gcs_credentials = None
    return gcs_credentials

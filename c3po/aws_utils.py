import io
import re
import json
import pandas as pd
from .aws_errors import S3Error


def body_parsers(object, filename, **kwargs):
    if re.search('.csv$', filename):
        parsed_body = pd.read_csv(
            io.BytesIO(object['Body'].read()), na_filter=kwargs['na_filter']
        )
    elif re.search('.json$', filename):
        parsed_body = json.load(object['Body'])
    else:
        raise S3Error.NoParserAvailable
    return parsed_body


def filter_files(all_keys, endswith=None):
    if endswith is not None:
        all_keys = filter(lambda x: str(x).endswith(endswith), all_keys)
    return list(all_keys)

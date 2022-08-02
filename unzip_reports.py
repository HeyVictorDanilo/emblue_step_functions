try:
    import unzip_requirements
except ImportError:
    pass

import boto3
import gzip
import os
from io import BytesIO
from botocore.exceptions import ClientError
import logging
import zipfile

from dotenv import load_dotenv
load_dotenv()


def handler(event, context):
    client = boto3.client(
        service_name='s3',
        region_name=os.getenv("REGION"),
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY")
    )

    try:
        response = client.list_objects(Bucket=os.getenv("BUCKET_NAME"))
    except ClientError as e:
        logging.error(e)
    else:
        for content in response.get("Contents"):
            file = BytesIO(client.get_object(Bucket=os.getenv("BUCKET_NAME"), Key=content.get("Key"))['Body'].read())
            _file = gzip.GzipFile(None, 'rb', fileobj=file)
            try:
                client.upload_fileobj(
                    Fileobj=_file,
                    Bucket=os.getenv("BUCKET_NAME"),
                    Key=content.get("Key").replace(".zip", "")
                )
            except ClientError as e:
                logging.error(e)
            else:
                logging.info("Uploaded unzipped file")
    return 1

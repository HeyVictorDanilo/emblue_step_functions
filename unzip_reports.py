try:
    import unzip_requirements
except ImportError:
    pass

import boto3
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
        delete_zip_files_data = []
        for content in response.get("Contents"):
            try:
                file = BytesIO(client.get_object(Bucket=os.getenv("BUCKET_NAME"), Key=content.get("Key"))['Body'].read())
            except ClientError as e:
                logging.error(e)
            else:
                delete_zip_files_data.append({"Key": content.get("Key")})
                _file = zipfile.ZipFile(file)
                for file_name in _file.namelist():
                    try:
                        client.upload_fileobj(
                            Fileobj=_file.open(file_name),
                            Bucket=os.getenv("BUCKET_NAME"),
                            Key=f'{file_name}'
                        )
                    except ClientError as e:
                        logging.error(e)
                    else:
                        logging.info("Uploaded unzipped file")
        delete_zip_files(data=delete_zip_files_data)

    return 1


def delete_zip_files(data):
    client = boto3.client(
        service_name='s3',
        region_name=os.getenv("REGION"),
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY")
    )
    try:
        response = client.delete_objects(
            Bucket=os.getenv("BUCKET_NAME"),
            Delete={"Objects": data},
        )
    except ClientError as e:
        logging.error(e)
    else:
        return response
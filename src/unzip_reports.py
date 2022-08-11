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


class ZipFile:
    def __init__(self, file_name):
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )
        self.file_name = file_name

    def executor(self):
        try:
            self.process_content(file_name=self.file_name)
        except Exception as e:
            return {
                "Error": e,
                "Description": "Something was wrong",
            }
        else:
            return {
                "Description": "Successfully execution",
            }

    def process_content(self, file_name):
        try:
            file = BytesIO(
                self.client.get_object(
                    Bucket=os.getenv("BUCKET_ZIP_FILES"), Key=file_name
                )["Body"].read()
            )
        except ClientError as e:
            logging.error(e)
        else:
            self.process_zip_file(_file=zipfile.ZipFile(file))
            self.delete_zip_file(file_name)

    def __get_account_name(self):
        return self.file_name.split("_")[0]

    def process_zip_file(self, _file):

        for file_name in _file.namelist():
            try:
                self.client.upload_fileobj(
                    Fileobj=_file.open(file_name),
                    Bucket=os.getenv("BUCKET_CSV_FILES"),
                    Key=f"{self.__get_account_name()}_{file_name}",
                )
            except ClientError as e:
                logging.error(e)
            else:
                logging.info("Uploaded unzipped file")

    def delete_zip_file(self, data):
        try:
            self.client.delete_object(
                Bucket=os.getenv("BUCKET_ZIP_FILES"),
                Key=self.file_name,
            )
        except ClientError as e:
            logging.error(e)
        else:
            logging.info("Deleted zip file")


def handler(event, context):
    zip_file = ZipFile(file_name=event["file_name"])
    response = zip_file.executor()
    return {
        "response": response
    }

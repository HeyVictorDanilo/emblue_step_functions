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


class ZipFiles:
    def __init__(self):
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )

    def executor(self):
        try:
            contents = self.get_file_contents()
            self.process_contents(contents=contents)
        except Exception as e:
            return {
                "Error": e,
                "Description": "Something was wrong",
            }
        else:
            return {
                "Description": "Successfully execution",
            }

    def get_file_contents(self):
        try:
            response = self.client.list_objects(Bucket=os.getenv("BUCKET_NAME"))
        except ClientError as e:
            logging.error(e)
        else:
            return response.get("Contents")

    def process_contents(self, contents):
        delete_zip_files_data = []
        for content in contents:
            try:
                file = BytesIO(
                    self.client.get_object(
                        Bucket=os.getenv("BUCKET_NAME"), Key=content.get("Key")
                    )["Body"].read()
                )
            except ClientError as e:
                logging.error(e)
            else:
                delete_zip_files_data.append({"Key": content.get("Key")})
                self.process_zip_file(_file=zipfile.ZipFile(file))
        self.delete_zip_files(data=delete_zip_files_data)

    def process_zip_file(self, _file):
        for file_name in _file.namelist():
            try:
                self.client.upload_fileobj(
                    Fileobj=_file.open(file_name),
                    Bucket=os.getenv("BUCKET_NAME"),
                    Key=f"{file_name}",
                )
            except ClientError as e:
                logging.error(e)
            else:
                logging.info("Uploaded unzipped file")

    def delete_zip_files(self, data):
        try:
            response = self.client.delete_objects(
                Bucket=os.getenv("BUCKET_NAME"),
                Delete={"Objects": data},
            )
        except ClientError as e:
            logging.error(e)
        else:
            return response


def handler(event, context):
    zip_files = ZipFiles()
    response = zip_files.executor()
    return {
        "response": response
    }

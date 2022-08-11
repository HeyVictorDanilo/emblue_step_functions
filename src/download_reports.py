try:
    import unzip_requirements
except ImportError:
    pass

from io import BytesIO
import os
import json
import logging

from botocore.exceptions import ClientError
from dotenv import load_dotenv
import boto3
import paramiko

load_dotenv()


def handler(event, context):
    sftp_file = SFTPFile(account=event["account"], date_file=event["file_date"])
    return {
        'response': sftp_file.download_file()
    }


class SFTPFile:
    def __init__(self, account, date_file):
        self.account = account
        self.date_file = date_file
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY")
        )

    def download_file(self):
        with BytesIO() as data:
            transport = paramiko.Transport(self.account[0], 22)
            transport.connect(username=self.account[1], password=self.account[2])
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.chdir(path="upload/Report")
                sftp.getfo(f"{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip", data)
                data.seek(0)
                try:
                    response = self.client.upload_fileobj(
                        data,
                        os.getenv("BUCKET_ZIP_FILES"),
                        f"{self.account[1]}_{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip"
                    )
                except ClientError as error:
                    logging.error(error)
                else:
                    return response

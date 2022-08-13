try:
    import unzip_requirements
except ImportError:
    pass

from io import BytesIO
import os
import time
import random
import logging

from botocore.exceptions import ClientError
from dotenv import load_dotenv
import boto3
import paramiko
from paramiko.ssh_exception import SSHException

from main_db import DBInstance

load_dotenv()


def handler(event, context):
    sftp_file = SFTPFile(account=event["account"], date_file=event["file_date"])
    return {
        'file_name': sftp_file.download_file()
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
            time.sleep(random.uniform(0.5, 5.5))
            try:
                transport = paramiko.Transport(self.account[0], 22)
                transport.connect(username=self.account[1], password=self.account[2])
            except SSHException as error:
                self.__write_log(
                    name=str(error),
                    description="Paramiko connection error",
                    is_bug=True
                )
            else:
                with paramiko.SFTPClient.from_transport(transport) as sftp:
                    sftp.chdir(path="upload/Report")
                    sftp.getfo(f"{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip", data)
                    data.seek(0)
                    try:
                        self.client.upload_fileobj(
                            data,
                            os.getenv("BUCKET_ZIP_FILES"),
                            f"{self.account[1]}_{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip"
                        )
                    except ClientError as error:
                        logging.error(error)
                        self.__write_log(
                            name=str(error),
                            description="Client error getting zip object",
                            is_bug=True
                        )
                    else:
                        self.__write_log(name="Download file", description="Successfully", is_bug=False)
                        return f"{self.account[1]}_{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip"

    def __write_log(self, name, description, is_bug = False):
        DBInstance.handler(query=f"""
            INSERT INTO em_blue_logs (name, description, account, file_name, is_bug)
                VALUES (
                    '{name}', 
                    '{description}', 
                    '{self.account[2]}', 
                    '{f"{os.getenv('FILE_BASE_NAME')}_{self.date_file}.zip"}', 
                    {is_bug}
                );
        """)
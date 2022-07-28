try:
    import unzip_requirements
except ImportError:
    pass

from main_db import DBInstance

import os
import paramiko
import logging
import boto3

from botocore.exceptions import ClientError
from datetime import date
from typing import List, Tuple, Any
from dotenv import load_dotenv

load_dotenv()


class ManageSFTPFile:
    def __init__(self, accounts, file_name):
        self.accounts = accounts
        self.file_name = file_name

    def download_files(self) -> None:
        for account in self.accounts:
            transport = paramiko.Transport(account[2], 22)
            transport.connect(username=account[4], password=account[3])
            client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("ACCESS_KEY"),
                aws_secret_access_key=os.getenv("SECRET_KEY"),
                region_name='us-east-1'
            )
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.chdir(path="upload/Report")
                with sftp.open(f"{self.file_name}.zip", "r") as f:
                    f.prefetch()
                    try:
                        response = client.put_object(Body=f, Bucket=os.getenv("BUCKET_NAME"), Key="file_name")
                    except ClientError as e:
                        print(e)
                        logging.error(e)


class Emblue:
    def __init__(self, searching_date: str = ""):
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        if searching_date:
            self.today = searching_date
        else:
            self.today = date.today().strftime("%Y%m%d")

    def get_emblue_accounts(self) -> List[Tuple[Any]]:
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def download_files(self):
        ManageSFTPFile(
            accounts=self.get_emblue_accounts(),
            file_name=f"{os.getenv('FILE_BASE_NAME')}_{self.today}"
        ).download_files()


def handler(event, context):
    emblue = Emblue()
    emblue.download_files()

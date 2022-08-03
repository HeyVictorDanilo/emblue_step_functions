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
from datetime import date, timedelta
from typing import List, Tuple, Any
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()


def handler(event, context):
    return {
        'sent_files': Emblue().download_files()
    }


class Emblue:
    def __init__(self, searching_date: str = ""):
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.client = boto3.client(
            service_name='s3',
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )
        if searching_date:
            self.today = searching_date
        else:
            self.today = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    def get_emblue_accounts(self) -> List[Tuple[Any]]:
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def download_files(self):
        manage_sftp_file = ManageSFTPFile(
            accounts=self.get_emblue_accounts(),
            file_name=f"{os.getenv('FILE_BASE_NAME')}_{self.today}",
            client=self.client
        )
        return manage_sftp_file.download_files()


class ManageSFTPFile:
    def __init__(self, accounts, file_name, client):
        self.accounts = accounts
        self.file_name = file_name
        self.client = client

    def download_files(self) -> List[str]:
        sent_files = []
        for account in self.accounts:
            transport = paramiko.Transport(account[2], 22)
            transport.connect(username=account[4], password=account[3])
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.chdir(path="upload/Report")
                with BytesIO() as data:
                    sftp.getfo(f"{self.file_name}.zip", data)
                    data.seek(0)
                    try:
                        response = self.client.upload_fileobj(
                            data,
                            os.getenv("BUCKET_NAME"),
                            f"{account[4]}_{self.file_name}.zip"
                        )
                        logging.info(response)
                        sent_files.append(f"{account[4]}_{self.file_name}.zip")
                    except ClientError as error:
                        logging.error(error)
                        continue
        return sent_files

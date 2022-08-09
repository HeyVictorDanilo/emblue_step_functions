try:
    import unzip_requirements
except ImportError:
    pass

from src.main_db import DBInstance

import os
import paramiko
import logging
import boto3

from botocore.exceptions import ClientError
from datetime import date, timedelta
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()


def handler(event, context):
    emblue = Emblue()
    return {
        'sent_files': emblue.get_files()
    }


class Emblue:
    def __init__(self, days_difference: int = 5):
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.client = boto3.client(
            service_name='s3',
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )

        self.days_difference = days_difference

    def __get_emblue_accounts(self):
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def get_files(self):
        sftp_instance = ManageSFTPFile(
            accounts=self.__get_emblue_accounts(),
            days_difference=self.days_difference,
            client=self.client
        )
        return sftp_instance.download_files()


class ManageSFTPFile:
    def __init__(self, accounts, days_difference, client):
        self.accounts = accounts
        self.days_difference = days_difference
        self.client = client
        self.send_files = []

    """
    @staticmethod
    def __establish_conn(account):
        transport = paramiko.Transport(account[2], 22)
        transport.connect(username=account[4], password=account[3])
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.chdir(path="upload/Report")
            return sftp
    """

    def download_files(self):
        for account in self.accounts:
            for i in range(0, self.days_difference):
                try:
                    response = self.__send_file(
                        date_file=(date.today() - timedelta(days=i)).strftime("%Y%m%d"),
                        account=account
                    )
                except ClientError as error:
                    logging.error(error)
                else:
                    self.send_files.append(response)
        return self.send_files

    def __send_file(self, date_file, account):
        with BytesIO() as data:
            transport = paramiko.Transport(account[2], 22)
            transport.connect(username=account[4], password=account[3])
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.chdir(path="upload/Report")
                sftp.getfo(f"{os.getenv('FILE_BASE_NAME')}_{date_file}.zip", data)
                data.seek(0)
                try:
                    response = self.client.upload_fileobj(
                        data,
                        os.getenv("BUCKET_ZIP_FILES"),
                        f"{account[4]}_{os.getenv('FILE_BASE_NAME')}_{date_file}.zip"
                    )
                except ClientError as error:
                    logging.error(error)
                else:
                    return response

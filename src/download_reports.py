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
    return {
        'sent_files': Emblue().get_files()
    }


class Emblue:
    def __init__(self, starting_date: str = "", finishing_date: str = ""):
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.client = boto3.client(
            service_name='s3',
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )
        if starting_date:
            self.starting_date = starting_date
        else:
            self.starting_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        if finishing_date:
            self.finishing_date = finishing_date
        else:
            self.finishing_date = date.today().strftime("%Y%m%d")

    def __get_emblue_accounts(self):
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def __date_range(self):
        return range(self.starting_date, self.finishing_date)

    def get_files(self):
        sftp_instance = ManageSFTPFile(
            accounts=self.__get_emblue_accounts(),
            date_range=self.__date_range(),
            client=self.client
        )
        return sftp_instance.download_files()


class ManageSFTPFile:
    def __init__(self, accounts, date_range, client):
        self.accounts = accounts
        self.date_range = date_range
        self.client = client
        self.send_files = []

    @staticmethod
    def __establish_conn(account):
        transport = paramiko.Transport(account[2], 22)
        transport.connect(username=account[4], password=account[3])
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            return sftp

    def download_files(self):
        for account in self.accounts:
            sftp = self.__establish_conn(account=account)
            sftp.chdir(path="upload/Report")
            for date_file in self.date_range:
                try:
                    response = self.__send_file(sftp_conn=sftp, date_file=date_file, account_name=account[4])
                except ClientError as error:
                    logging.error(error)
                else:
                    self.send_files.append(response)
        return self.send_files

    def __send_file(self, sftp_conn, date_file, account_name):
        with BytesIO() as data:
            sftp_conn.getfo(f"{os.getenv('FILE_BASE_NAME')}_{date_file}.zip", data)
            data.seek(0)
            try:
                response = self.client.upload_fileobj(
                    data,
                    os.getenv("BUCKET_ZIP_FILES"),
                    f"{account_name}_{os.getenv('FILE_BASE_NAME')}_{date_file}.zip"
                )
            except ClientError as error:
                logging.error(error)
            else:
                return response

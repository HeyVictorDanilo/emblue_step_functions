try:
    import unzip_requirements
except ImportError:
    pass

from datetime import date

import boto3
import os
from io import BytesIO
from botocore.exceptions import ClientError
import logging
import zipfile

from dotenv import load_dotenv
from src.main_db import DBInstance

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
            self.__write_log(message=e, status=1)
            return {
                "Error": str(e),
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
            self.__write_log(message=e, status=1)
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
                self.__write_log(message=e, status=1)
            else:
                logger.info(f"Uploaded unzipped file: {f'{self.__get_account_name()}_{file_name}'}")

    def delete_zip_file(self, data):
        try:
            self.client.delete_object(
                Bucket=os.getenv("BUCKET_ZIP_FILES"),
                Key=self.file_name,
            )
        except ClientError as e:
            logger.error(e)
        else:
            logger.info("Deleted zip file")

    def __get_account_values(self):
        db = DBInstance(os.getenv("CLIENT_KEY"))
        result = db.handler(query=f"""
            SELECT 
                id, 
                emb.migrate_open_email, 
                emb.migrate_link_click, 
                emb.migrate_unsubscribe,
                emb.migrate_sent_email 
            FROM 
                em_blue 
            WHERE 
                emblue_user = '{self.__get_account_name()}';"""
            )
        return result

    def __write_log(self, message, status):
        db = DBInstance(os.getenv("CLIENT_KEY"))
        account_values = self.__get_account_values()

        if account_values[1]:
            db.handler(query=f"""
                INSERT INTO em_blue_migration_log (date_migrated, account_id, event_migrated, file_name, status, 
                    message, created_at
                )
                VALUES ('{date.today()}', {account_values[0]}, 0, '{self.file_name}, {status}, '{str(message)}', 
                    '{date.today()}');
                """
            )

        if account_values[2]:
            db.handler(query=f"""
                INSERT INTO em_blue_migration_log (date_migrated, account_id, event_migrated, file_name, status, 
                    message, created_at
                )
                VALUES (
                    '{date.today()}', {account_values[0]}, 1, '{self.file_name}, {status}, '{str(message)}', 
                    '{date.today()}');
                """
            )

        if account_values[3]:
            db.handler(query=f"""
                INSERT INTO em_blue_migration_log (date_migrated, account_id, event_migrated, file_name, status, 
                    message, created_at
                )
                VALUES (
                    '{date.today()}', {account_values[0]}, 2, '{self.file_name}, {status}, '{str(message)}', 
                    '{date.today()}');
                """
            )

        if account_values[4]:
            db.handler(query=f"""
                INSERT INTO em_blue_migration_log (date_migrated, account_id, event_migrated, file_name, status, 
                    message, created_at
                )
                VALUES (
                    '{date.today()}', {account_values[0]}, 3, '{self.file_name}, {status}, '{str(message)}', 
                    '{date.today()}');
                """
            )


def handler(event, context):
    try:
        zip_file = ZipFile(file_name=event["file_name"])
        response = zip_file.executor()
    except Exception as e:
        logger.error(e)
    else:
        logger.info(f"Processing file: {event['file_name']}")
        return {
            "response": response
        }

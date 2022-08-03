try:
    import unzip_requirements
except ImportError:
    pass

from multiprocessing import Process
from itertools import islice
import os
import logging

import boto3
from botocore.exceptions import ClientError

from src.main_db import DBInstance


class ProcessFiles:
    def __init__(self):
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.processes = []

    def executor(self):
        for content in self.get_file_contents():
            p = Process(target=self.process_file, args=(content.get("Key"), ))
            self.processes.append(p)

        for p in self.processes:
            p.start()

        for p in self.processes:
            p.join()

    def get_file_contents(self):
        try:
            response = self.client.list_objects(Bucket=os.getenv("BUCKET_NAME"))
        except ClientError as e:
            logging.error(e)
        else:
            return response.get("Contents")

    def process_file(self, file_name: str):
        file = self.client.get_object(
            Bucket=os.getenv("BUCKET_NAME"), Key=file_name
        )["Body"].read().decode('utf-16').splitlines()

        while True:
            lines = list(islice(file, 1000))
            self.process_lines(lines=lines)
            if not lines:
                break

        #os.remove(file_name)


    def process_lines(self, lines):
        sent_values_list = []
        click_values_list = []
        open_values_list = []
        unsubscribe_values_list = []

        for line in lines:
            line_words = line.split(";")
            if not line_words[8]:
                tag = "NULL"
            else:
                tag = line_words[8]

            if line_words[6] == "Enviado":
                sent_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag,
                    )
                )

            if line_words[6] == "Click":
                click_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag,
                    )
                )

            if line_words[6] == "Abierto":
                open_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag,
                    )
                )

            if line_words[6] == "Desuscripto":
                unsubscribe_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag,
                    )
                )

            if line_words[6] == "Rebote":
                pass

        if sent_values_list:
            build_insert_sent_query = self.build_insert_query(
                table="em_blue_sent_email_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag",
                ],
                values=sent_values_list,
            )
            self.db_instance.handler(query=build_insert_sent_query)

        if click_values_list:
            build_insert_click_query = self.build_insert_query(
                table="em_blue_link_click_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "url",
                    "tag",
                ],
                values=click_values_list,
            )
            self.db_instance.handler(query=build_insert_click_query)

        if open_values_list:
            build_insert_open_query = self.build_insert_query(
                table="em_blue_open_email_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag",
                ],
                values=open_values_list,
            )
            self.db_instance.handler(query=build_insert_open_query)

        if unsubscribe_values_list:
            build_insert_unsubscribe_query = self.build_insert_query(
                table="em_blue_unsubscribe_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag",
                ],
                values=unsubscribe_values_list,
            )
            self.db_instance.handler(query=build_insert_unsubscribe_query)

    @staticmethod
    def build_insert_query(table: str, columns, values) -> str:
        return f"""
            INSERT INTO {table}({", ".join([str(c) for c in columns])})
            VALUES {values};
        """.replace(
            "[", ""
        ).replace(
            "]", ""
        )


def handler(event, context):
    process_instance = ProcessFiles()
    process_instance.executor()
    return {"response": 1}

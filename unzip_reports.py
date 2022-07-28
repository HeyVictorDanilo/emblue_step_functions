try:
  import unzip_requirements
except ImportError:
  pass


import zipfile
import os


class ManageZip:
    def __init__(self, file_name):
        self.file_name = file_name

    def unzip_local_file(self) -> None:
        try:
            with zipfile.ZipFile(
                f"{self.file_name}.zip", mode="r"
            ) as archive:
                archive.extractall()
        except zipfile.BadZipFile as error:
            raise error
        finally:
            os.remove(f"{self.file_name}.zip")


def handler(event, context):
    print("Executing unzip")

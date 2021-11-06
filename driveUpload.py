import shutil

from application_logging import logger
from os import listdir
import os
from flask import Response
from os.path import isfile, join
from Google import Create_Service
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from io import BytesIO
import pandas as pd

class upload_training:
    def __init__(self, path):
        self.path = path
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.file_object2 = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.client = os.getcwd()+"/client_secret_431546631695-2v7rjgrllsp7s388h97a53tf43d738lj.apps.googleusercontent.com.json"
        self.api = "drive"
        self.api_version = "v3"
        self.scope = ["https://www.googleapis.com/auth/drive"]
        self.train_dir = "1R-POm3OOtkA5p8GiWxqnEqVjb37o7KXe"
        self.predict_dir = "1_3gsR3Twmm9Hd2yGe-h4mse6evGjdh3M"
        self.predictions = "153MXM0bgiGerZqVZLVrjC6gqC3T2Cdn9"


    def uploadfile_train(self):
        try:
            service = Create_Service(self.client, self.api, self.api_version, self.scope)
            self.log_writer.log(self.file_object, 'entered uploadfile_train of driveUpload.py!!')

            filenames = [f for f in listdir(self.path)]
            if not filenames:
                raise AssertionError("File Not Found!!!")
            else:
                path = '/Training_Batch_Files'
                if os.path.isdir(os.getcwd()+path):
                    shutil.rmtree(os.getcwd()+path)

                if not os.path.isdir(os.getcwd()+path):
                    os.makedirs(os.getcwd()+path)
                self.delete_existing_training()
                self.create_new_training()
                service.files().emptyTrash().execute()



                #need to download gcp credential file
                #gcpcred_path = "path of that file"

                #storage_client = storage.Client(gcpcred_path)

                query = f"parents = '{self.train_dir}'"
                response = service.files().list(q=query).execute()
                files = response.get('files')

                dir = ""
                for f in files:
                    if (f['name'] == 'Training_Batch_Files'):
                        dir = f['id']


                # bucket = storage_client.get_bucket('bucket name')

                for i in filenames:
                    for j in range(0, 1):
                        file_metadata = {
                            'name': i,
                            'parents': [dir],
                            'mimetype': 'application/vnd.google-apps.spreadsheet'
                        }

                        media_content = MediaFileUpload(self.path + '/' + i, mimetype="text/csv", resumable=True)

                        file = service.files().create(
                            body=file_metadata,
                            media_body=media_content
                        ).execute()
            self.log_writer.log(self.file_object, 'entered uploadfile_train phase2 of driveUpload.py!!')
            query = f"parents = '{dir}'"
            response = service.files().list(q=query).execute()
            files = response.get('files')

            for f in files:
                request = service.files().get_media(fileId=f['id'])
                file = BytesIO()
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while done is False: _, done = downloader.next_chunk()
                file.seek(0)
                df = pd.read_csv(file)
                df.to_csv(os.getcwd()+"/Training_Batch_Files/" + f['name'], index=None, header=True)
                self.log_writer.log(self.file_object, 'entered uploadfile_train phase2 loop of driveUpload.py!!')


                    #blob = bucket.blob("Training/"+i)
                    #blob.upload_from_filename(j)

            self.log_writer.log(self.file_object, 'exited uploadfile_train of driveUpload.py!!')
        except Exception as e:
            print(e)
            raise e

    def uploadfile_predict(self):
        try:
            service = Create_Service(self.client, self.api, self.api_version, self.scope)
            self.log_writer.log(self.file_object2, 'entered uploadfile_train of driveUpload.py!!')

            filenames = [f for f in listdir(self.path)]
            if not filenames:
                raise AssertionError("File Not Found!!!")
            else:
                path = '/Prediction_Batch_Files'
                if os.path.isdir(os.getcwd() + path):
                    shutil.rmtree(os.getcwd() + path)

                if not os.path.isdir(os.getcwd()+path):
                    os.makedirs(os.getcwd()+path)
                self.delete_existing_prediction()
                self.create_new_prediction()
                service.files().emptyTrash().execute()

                # need to download gcp credential file
                # gcpcred_path = "path of that file"

                # storage_client = storage.Client(gcpcred_path)

                query = f"parents = '{self.predict_dir}'"
                response = service.files().list(q=query).execute()
                files = response.get('files')

                dir = ""
                for f in files:
                    if (f['name'] == 'Prediction_Batch_Files'):
                        dir = f['id']

                # bucket = storage_client.get_bucket('bucket name')

                for i in filenames:
                    for j in range(0, 1):
                        file_metadata = {
                            'name': i,
                            'parents': [dir],
                            'mimetype': 'application/vnd.google-apps.spreadsheet'
                        }

                        media_content = MediaFileUpload(self.path + '/' + i, mimetype="text/csv", resumable=True)

                        file = service.files().create(
                            body=file_metadata,
                            media_body=media_content
                        ).execute()
            self.log_writer.log(self.file_object2, 'entered uploadfile_predict phase2 of driveUpload.py!!')
            query = f"parents = '{dir}'"
            response = service.files().list(q=query).execute()
            files = response.get('files')

            for f in files:
                request = service.files().get_media(fileId=f['id'])
                file = BytesIO()
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while done is False: _, done = downloader.next_chunk()
                file.seek(0)
                df = pd.read_csv(file)
                df.to_csv(os.getcwd() + "/Prediction_Batch_Files/" + f['name'], index=None, header=True)
                self.log_writer.log(self.file_object2, 'entered uploadfile_prediction phase2 loop of driveUpload.py!!')


            #blob = bucket.blob("Prediction/" + i)
                    #blob.upload_from_filename(j)

            self.log_writer.log(self.file_object, 'exited uploadfile_predict of driveUpload.py!!')
        except Exception as e:
            print(e)
            raise e

    def delete_existing_training(self):
        self.log_writer.log(self.file_object, 'entered delete existing training of driveUpload.py!!')

        service = Create_Service(self.client, self.api, self.api_version, self.scope)

        query = f"parents = '{self.train_dir}'"

        response = service.files().list(q=query).execute()
        files = response.get('files')

        for f in files:
            service.files().delete(fileId=f['id']).execute()
        service.files().emptyTrash().execute()

    def delete_existing_prediction(self):
        self.log_writer.log(self.file_object2, 'entered delete existing prediction of driveUpload.py!!')
        service = Create_Service(self.client, self.api, self.api_version, self.scope)

        query = f"parents = '{self.predict_dir}'"

        response = service.files().list(q=query).execute()
        files = response.get('files')

        for f in files:
            service.files().delete(fileId=f['id']).execute()
        service.files().emptyTrash().execute()

    def create_new_training(self):
        self.log_writer.log(self.file_object, 'entered create training folders of driveUpload.py!!')
        service = Create_Service(self.client, self.api, self.api_version, self.scope)

        folder = ['Training_Batch_Files']
        # for i in range(0, len(folder)):
        file_metadata = {
            'name': folder[0],
            "parents": ["1R-POm3OOtkA5p8GiWxqnEqVjb37o7KXe"],
            'mimeType': 'application/vnd.google-apps.folder'
        }
        service.files().create(body=file_metadata).execute()

    def create_new_prediction(self):
        self.log_writer.log(self.file_object2, 'entered create prediction folders of driveUpload.py!!')
        service = Create_Service(self.client, self.api, self.api_version, self.scope)

        folder = ['Prediction_Batch_Files']
        # for i in range(0, len(folder)):
        file_metadata = {
            'name': folder[0],
            "parents": ["1_3gsR3Twmm9Hd2yGe-h4mse6evGjdh3M"],
            'mimeType': 'application/vnd.google-apps.folder'
        }
        service.files().create(body=file_metadata).execute()

    def upload_Prediction(self):
        try:
            service = Create_Service(self.client, self.api, self.api_version, self.scope)
            self.log_writer.log(self.file_object2, 'entered upload_prediction of driveUpload.py!!')

            service.files().emptyTrash().execute()
            filenames = [f for f in listdir(self.path)]
            if not filenames:
                raise AssertionError("File Not Found!!!")
            else:

                for i in filenames:
                    for j in range(0, 1):
                        file_metadata = {
                            'name': i,
                            'parents': [self.predictions],
                            'mimetype': 'application/vnd.google-apps.spreadsheet'
                        }

                        media_content = MediaFileUpload(self.path + i, mimetype="text/csv", resumable=True)

                        file = service.files().create(
                            body=file_metadata,
                            media_body=media_content
                        ).execute()
        except Exception as e:
            return e



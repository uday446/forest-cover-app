import shutil
import requests
import io
import time
from application_logging import logger
from github import Github
import os
import pandas as pd

class upload_training:
    def __init__(self, path):
        self.path = path
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.file_object2 = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()



    def uploadfile_train(self):
        try:
            self.log_writer.log(self.file_object, 'entered uploadfile_train of driveUpload.py!!')

            if os.path.isdir(os.getcwd()+ "/Training_Batch_Files"):
                shutil.rmtree(os.getcwd()+ "/Training_Batch_Files")

            if not os.path.isdir(os.getcwd()+ "/Training_Batch_Files"):
                os.makedirs(os.getcwd()+ "/Training_Batch_Files")

            username = os.environ.get("GITUSER")
            # Personal Access Token (PAO) from your GitHub account
            token = os.environ.get("GITTOCKEN")
            # Creates a re-usable session object with your creds in-built
            github_session = requests.Session()
            github_session.auth = (username, token)  # Downloading the csv file from your GitHub
            url = self.path  # Make sure the url is the raw version of the
            download = github_session.get(url).content
            # Reading the downloaded content and making it a pandas dataframe
            df = pd.read_csv(io.StringIO(download.decode('utf-8')))

            timestr = time.strftime("%Y%m%d_%H%M%S")

            df.to_csv(os.getcwd()+"/Training_Batch_Files/" + "forest_cover_"+timestr+".csv", index=None, header=True)

            self.log_writer.log(self.file_object, 'exited uploadfile_train of driveUpload.py!!')
        except Exception as e:
            print(e)
            raise e

    def uploadfile_predict(self):
        try:
            self.log_writer.log(self.file_object2, 'entered uploadfile_predict of driveUpload.py!!')

            if os.path.isdir("Prediction_Batch_Files"):
                shutil.rmtree("Prediction_Batch_Files")

            if not os.path.exists("Prediction_Batch_Files"):
                #os.makedirs(os.getcwd() + "/Prediction_Batch_Files", exist_ok=True)
                os.mkdir("Prediction_Batch_Files")

            username = os.environ.get("GITUSER")
            # Personal Access Token (PAO) from your GitHub account
            token = os.environ.get("GITTOCKEN")
            # Creates a re-usable session object with your creds in-built
            github_session = requests.Session()
            github_session.auth = (username, token)  # Downloading the csv file from your GitHub
            url = self.path  # Make sure the url is the raw version of the
            download = github_session.get(url).content
            # Reading the downloaded content and making it a pandas dataframe
            df = pd.read_csv(io.StringIO(download.decode('utf-8')))

            timestr = time.strftime("%Y%m%d_%H%M%S")

            name = "forest_cover_" + timestr + ".csv"
            #path = os.path.join(os.getcwd(),"/Prediction_Batch_Files/")
            #path = os.path.join(path, name)

            #os.mkdir("Prediction_Batch_Files")
            path = "Prediction_Batch_Files/"+name

            df.to_csv(name, index=None, header=True, mode='w')
            shutil.copy(name, "Prediction_Batch_Files")

            self.log_writer.log(self.file_object, 'exited uploadfile_predict of driveUpload.py!!')
        except Exception as e:
            print(e)
            raise e

    def upload_Prediction(self):
        try:
            self.log_writer.log(self.file_object2, 'entered upload_prediction of driveUpload.py!!')
            token = os.environ.get("GITTOCKEN")
            g = Github(token)

            repo = g.get_user().get_repo('forest-cover-app')
            all_files = []
            contents = repo.get_contents("")
            while contents:
                file_content = contents.pop(0)
                if file_content.type == "dir":
                    contents.extend(repo.get_contents(file_content.path))
                else:
                    file = file_content
                    all_files.append(str(file).replace('ContentFile(path="', '').replace('")', ''))

            with open(os.getcwd() + '/Prediction_Output_File/Predictions.csv', 'r') as file:
                content = file.read()

            # Upload to github
            git_prefix = 'Prediction_Output_File'
            git_file = git_prefix + '.csv'
            if git_file in all_files:
                contents = repo.get_contents(git_file)
                repo.update_file(contents.path, "committing files", content, contents.sha, branch="main")
                print(git_file + ' UPDATED')
            else:
                repo.create_file(git_file, "committing files", content, branch="main")
                print(git_file + ' CREATED')

        except Exception as e:
            return e



import shutil
from wsgiref import simple_server
from flask import Flask, request, render_template
from flask_cors import CORS,cross_origin
from flask import Response
import os
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction
import flask_monitoringdashboard as dashboard
from driveUpload import upload_training
import threading
from mailer import mail

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)

dashboard.bind(app)
CORS(app)

mailing = mail()

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.form is not None:
            path = request.form['filepath']

            if not os.path.exists("Prediction_Batch"):
                # os.makedirs(os.getcwd() + "/Prediction_Batch_Files", exist_ok=True)
                os.mkdir("Prediction_Batch")

            folder = 'Prediction_Batch'
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    raise e
                    print('Failed to delete %s. Reason: %s' % (file_path, e))

            #predict_upload = gcp(path)
            #predict_upload.uploadfile_predict()
            predict_upload = upload_training(path)
            predict_upload.uploadfile_predict()

            threading.Thread(target=prediction_task).start()

            return Response("Please Wait While The Prediction File is Getting Created At %s!!!" % path)

    except ValueError:
        mailing.send_mail(str(ValueError))
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        mailing.send_mail(str(KeyError))
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        mailing.send_mail(str(e))
        return Response("Error Occurred! %s" %e)

def prediction_task():
    try:
        """
        Background Process...
        """
        path = 'Prediction_Batch/'
        pred_val = pred_validation(path)  # object initialization

        pred_val.prediction_validation()  # calling the prediction_validation function

        pred = prediction(path)  # object initialization

        # predicting for dataset present in database
        path = pred.predictionFromModel()

    except Exception as e:
        raise e


@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.form is not None:
            path = request.form['filepath']

            #train_upload = gcp(path)
            #train_upload.uploadfile_train()


            if not os.path.exists("Training_Batch"):
                # os.makedirs(os.getcwd() + "/Prediction_Batch_Files", exist_ok=True)
                os.mkdir("Training_Batch")

            folder = 'Training_Batch'
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    raise e
                    print('Failed to delete %s. Reason: %s' % (file_path, e))

            train_upload = upload_training(path)
            train_upload.uploadfile_train()
            threading.Thread(target=train_task).start()

            return Response("Please Wait While The Prediction File is Getting Created At %s!!!" % path)

    except ValueError:
        mailing.send_mail(str(ValueError))
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        mailing.send_mail(str(KeyError))
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        mailing.send_mail(str(e))
        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

def train_task():
    try:

        path = 'Training_Batch/'
        train_valObj = train_validation(path)  # object initialization

        train_valObj.train_validation()  # calling the training_validation function

        trainModelObj = trainModel()  # object initialization
        trainModelObj.trainingModel()  # training the model for the files in the table
    except Exception as e:
        raise e


port = int(os.getenv("PORT",5001))
if __name__ == "__main__":
    host = '0.0.0.0'
    # port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()

import shutil
import sqlite3
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from os import listdir
import os
import csv
from application_logging.logger import App_Logger
import json

class dBOperation:
    """
          This class shall be used for handling all the SQL operations.

          Written By: iNeuron Intelligence
          Version: 1.0
          Revisions: None

          """

    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()


    def dataBaseConnection(self,DatabaseName):

        """
                        Method Name: dataBaseConnection
                        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                        Output: Connection to the DB
                        On Failure: Raise ConnectionError

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                        """
        try:
            #conn = sqlite3.connect(self.path+DatabaseName+'.db')

            cloud_config = {
                'secure_connect_bundle': os.getcwd()+'/secure-connect-internship.zip'
            }
            auth_provider = PlainTextAuthProvider(os.environ.get("dbstr1"), os.environ.get("dbstr2"))
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect('project')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return session

    def createTableDb(self,DatabaseName,column_names):

        """
           Method Name: createTableDb
           Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
           Output: None
           On Failure: Raise Exception

            Written By: iNeuron Intelligence
           Version: 1.0
           Revisions: None

        """
        try:
            conn = self.dataBaseConnection("project")
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE project.Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE IF NOT EXISTS project.Good_Raw_Data (id UUID PRIMARY KEY, elevation INT, aspect INT, slope INT, horizontal_distance_to_hydrology INT, Vertical_Distance_To_Hydrology INT, Horizontal_Distance_To_Roadways INT, Horizontal_Distance_To_Fire_Points INT, wilderness_area1 INT, wilderness_area2 INT, wilderness_area3 INT, wilderness_area4 INT, soil_type_1 INT, soil_type_2 INT, soil_type_3 INT, soil_type_4 INT, soil_type_5 INT, soil_type_6 INT, soil_type_7 INT, soil_type_8 INT, soil_type_9 INT, soil_type_10 INT, soil_type_11 INT, soil_type_12 INT, soil_type_13 INT, soil_type_14 INT, soil_type_15 INT, soil_type_16 INT, soil_type_17 INT, soil_type_18 INT, soil_type_19 INT, soil_type_20 INT, soil_type_21 INT, soil_type_22 INT, soil_type_23 INT, soil_type_24 INT, soil_type_25 INT, soil_type_26 INT, soil_type_27 INT, soil_type_28 INT, soil_type_29 INT, soil_type_30 INT, soil_type_31 INT, soil_type_32 INT, soil_type_33 INT, soil_type_34 INT, soil_type_35 INT, soil_type_36 INT, soil_type_37 INT, soil_type_38 INT, soil_type_39 INT, soil_type_40 INT)'.format(column_name=key, dataType=type))



            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database,key):

        """
                                       Method Name: insertIntoTableGoodData
                                       Description: This method inserts the Good data files from the Good_Raw folder into the
                                                    above created table.
                                       Output: None
                                       On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                       Version: 1.0
                                       Revisions: None

                """

        conn = self.dataBaseConnection(Database)
        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        column=key.keys()

        for file in onlyfiles:
            try:

                with open(goodFilePath+'/'+file, "r") as f:
                    reader = csv.reader(f, delimiter="\n")
                    next(f)
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                query = 'INSERT INTO project.Good_Raw_Data (id, elevation, aspect, slope, horizontal_distance_to_hydrology, Vertical_Distance_To_Hydrology, Horizontal_Distance_To_Roadways, Horizontal_Distance_To_Fire_Points, wilderness_area1, wilderness_area2, wilderness_area3, wilderness_area4, soil_type_1, soil_type_2, soil_type_3, soil_type_4, soil_type_5, soil_type_6, soil_type_7, soil_type_8, soil_type_9, soil_type_10, soil_type_11, soil_type_12, soil_type_13, soil_type_14, soil_type_15, soil_type_16, soil_type_17, soil_type_18, soil_type_19, soil_type_20, soil_type_21, soil_type_22, soil_type_23, soil_type_24, soil_type_25, soil_type_26, soil_type_27, soil_type_28, soil_type_29, soil_type_30, soil_type_31, soil_type_32, soil_type_33, soil_type_34, soil_type_35, soil_type_36, soil_type_37, soil_type_38, soil_type_39, soil_type_40) values (uuid(),{values})'.format(values=(list_))
                                conn.execute(query)
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                print(list_)
                            except Exception as e:
                                raise e

            except Exception as e:


                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()

                raise e


        log_file.close()


    def selectingDatafromtableintocsv(self,Database,keys):

        """
                                       Method Name: selectingDatafromtableintocsv
                                       Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                                    above created .
                                       Output: None
                                       On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                       Version: 1.0
                                       Revisions: None

                """

        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            columns = list(keys.keys())
            joined = ",".join(columns)

            sqlSelect = "SELECT {columns} FROM Good_Raw_Data".format(columns=joined)


            results = conn.execute(sqlSelect)

            #Get the headers of the csv file
            headers = keys.keys()

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            raise e






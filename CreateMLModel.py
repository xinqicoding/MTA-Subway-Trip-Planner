# Creating aws machine learning model
# This program uploads the finalData.csv file to S3, and used it as a data source to train a binary
# classification model
import time,sys,random
import boto3
from S3 import S3

sys.path.append('../utils')

import aws

client = aws.getClient('machinelearning','us-east-1')
TIMESTAMP  =  time.strftime('%Y-%m-%d-%H-%M-%S')
S3_BUCKET_NAME = "mtaedisondataiottue"
S3_FILE_NAME = 'trainRecordFinal.csv'
S3_URI = "s3://mtaedisondataiottue/trainRecordFinal.csv"
DATA_SCHEMA ='{"excludedAttributeNames": [], "version": "1.0", "dataFormat": "CSV", "dataFileContainsHeader": true,"attributes": [{"attributeName": "minutes", "attributeType": "NUMERIC"}, {"attributeName": "tripId", "attributeType": "TEXT"}, {"attributeName": "route", "attributeType": "CATEGORICAL"}, {"attributeName": "DAY", "attributeType": "CATEGORICAL"}, {"attributeName": "expressArrivalTime", "attributeType": "NUMERIC"}, {"attributeName": "destArrivalTime", "attributeType": "NUMERIC"}, {"attributeName": "decision", "attributeType": "BINARY"}], "targetAttributeName": "decision"}'
DATA_SOURCE_ID = 'lab5datasource'
ML_MODEL_ID = 'lab5mlmodel'
EVALUATION_ID = 'lab5evaluation'

testvariable = S3(S3_FILE_NAME)
testvariable.uploadData()

response = client.create_data_source_from_s3(
    DataSourceId=DATA_SOURCE_ID,
    DataSourceName='lab5',
    DataSpec={
        'DataLocationS3': S3_URI,
        'DataSchema': DATA_SCHEMA
    },
    ComputeStatistics=True
)
response1 = client.create_ml_model(
    MLModelId= ML_MODEL_ID,
    MLModelName='lab5ml',
    MLModelType='BINARY',
    TrainingDataSourceId= DATA_SOURCE_ID
)

response = client.create_evaluation(
    EvaluationId=EVALUATION_ID,
    EvaluationName='lab5eva',
    MLModelId=ML_MODEL_ID,
    EvaluationDataSourceId=DATA_SOURCE_ID
)

response = client.get_evaluation(
    EvaluationId= EVALUATION_ID
)


# Creating aws machine learning model
# This program uploads the finalData.csv file to S3, and used it as a data source to train a binary 
# classification model
import time,sys,random

import boto3

import S3

sys.path.append('../utils')
import aws

TIMESTAMP  =  time.strftime('%Y-%m-%d-%H-%M-%S')
S3_BUCKET_NAME = "<YOUR S3 BUCKET NAME HERE>"
S3_FILE_NAME = 'finalData.csv'
S3_URI = "s3://{0}/{1}".format(S3_BUCKET_NAME, S3_FILE_NAME)
DATA_SCHEMA = "aml.csv.schema"










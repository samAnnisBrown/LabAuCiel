import csv
import boto3
import os
import re

from gzip import GzipFile
from io import BytesIO
from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

# Global Variables
esHost = "search-wwes-75dyceauwq2lk6pg3kf5w4254y.ap-southeast-2.es.amazonaws.com"   # Elasticsearch Domain
uploadToEs = True               # Set to false for dry-run (will not impact ES domain)
lambdaAuth = True               # Set to True if running in a Lambda function
totalLinesUploadedCount = 0     # Do not modify
totalLinesCount = 0             # Do not modify


def handler(event, context):

    # Retrieve S3 object from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Download S3 file
    s3 = boto3.client('s3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    # TODO use scratch space on disk instead? Lambda has only 500Mb though :(
    bytestream = BytesIO(s3file['Body'].read())
    outfile = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    # Build Index Name
    reportMonth = re.search(".*/(\d+-\d+)/", key).group(1).split("-")[0][:-2]
    reportName = (re.search(".*/(.+?)/\d+-\d+/", key)).group(1)
    indexName = ("cur-" + str(reportName) + "-" + str(reportMonth)).lower()

    # Remove existing index with same name (to avoid duplicate entries)
    if uploadToEs is True:
        deleteElasticsearchIndex(indexName)

    # Prepare variables
    linesToUpload = []
    global totalLinesCount
    totalLinesCount = len(outfile.splitlines())

    # Parse and upload file contents
    for count, line in enumerate(outfile.splitlines(), 1):

        if count == 1:  # Header Row: retrieve field names
            payloadKeysIn = list(csv.reader([line]))[0] # need to encapsulate/decapsulate list for csv.reader to work

            # Replace '/' with '_' for field names.  Makes life easier in Elasticsearch
            payloadKeys = []
            for value in payloadKeysIn:
                payloadKeys.append(value.replace("/", "_"))

            # Determine index location for certain columns to be Integers or Floats
            forceInteger = [index for index, string in enumerate(payloadKeys) if 'engine' in string
                            or 'Iopsvol' in string
                            or 'vcpu' in string
                            or 'UnitsPerReservation' in string
                            or 'TotalReservedUnits' in string]
            forceFloat = [index for index, string in enumerate(payloadKeys) if 'UsageAmount' in string
                          or 'lended' in string
                          or 'SizeFactor' in string
                          or 'OnDemand' in string]

        else:
            # Report Body
            payloadValuesOut = []
            payloadValuesRaw = list(csv.reader([line]))[0]

            # Convert integers and floats to numbers in the output JSON
            for index, value in enumerate(payloadValuesRaw):
                if index in forceInteger:
                    try:
                        payloadValuesOut.append(int(value))
                    except:
                        payloadValuesOut.append(0)
                elif index in forceFloat:
                    try:
                        payloadValuesOut.append(float(value))
                    except:
                        payloadValuesOut.append(0.0)
                else:
                    # Anything that's not integer or float is created as a String
                    payloadValuesOut.append(value)

            # Verify that the output row length equals the header row length (otherwise we have a column mismatch)
            if len(payloadValuesOut) != len(payloadKeys):
                print("Line " + str(count) + " is " + str(len(payloadValuesOut)) + ". Should be " + str(len(payloadKeys)) + "." + " -- " + str(payloadValuesOut))
            else:
                # Created the individual line payload
                payload = dict(zip(payloadKeys, payloadValuesOut))

                # Create the required JSON for Elasticsearch upload
                linesToUpload.append({"_index": indexName, "_type": "CostReport", "_source": payload})

                # If linesToUpload is > 1000, complete a bulk upload
                if len(linesToUpload) >= 1000:
                    uploadToElasticsearch(linesToUpload)
                    linesToUpload = []

    # If there are any lines left once loop is completed, upload them.
    if len(linesToUpload) > 0:
        uploadToElasticsearch(linesToUpload)


def uploadToElasticsearch(actions):

    global totalLinesUploadedCount

    if uploadToEs:
        es = returnElasticsearchAuth()
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)

        helpers.bulk(es, actions)
        print("Uploaded " + str(len(actions)) + " lines  - " + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded. (" + str(percent) + "%)")
    else:
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)
        print("Upload set to 'False'.  Would've uploaded " + str(len(actions)) + " lines -  " + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded. (" + str(percent) + "%)")


def listElasticsearchIndices():
    es = returnElasticsearchAuth()
    print(es.indices.get_alias("*"))


def deleteElasticsearchIndex(indexName):
    es = returnElasticsearchAuth()
    es.indices.delete(index=indexName, ignore=[400, 404])


def returnElasticsearchAuth():

    if lambdaAuth:
        # Retrieve Access details (Lambda IAM role must have access to ES domain to work)
        awsauth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                                  aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                  aws_token=os.environ['AWS_SESSION_TOKEN'],
                                  aws_host=esHost,
                                  aws_region='ap-southeast-2',
                                  aws_service='es')
    else:
        # If running outside of a Lambda function, retrieve creds using standard BOTO logic
        awsauth = BotoAWSRequestsAuth(aws_host=esHost,
                                      aws_region='ap-southeast-2',
                                      aws_service='es')

    es = Elasticsearch(host=esHost,
                       port=80,
                       connection_class=RequestsHttpConnection,
                       http_auth=awsauth)

    return es


if not lambdaAuth:  # If not in a Lambda, launch main function and pass S3 event JSON
    handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "ansamual-costreports",
                    },
                    "object": {
                        "key": "QuickSight_RedShift/QuickSight_RedShift_CostReports/20171201-20180101/1934845f-ade9-404e-b3c0-84eee5a729d4/QuickSight_RedShift_CostReports-1.csv.gz",
                    }
                }
            }
        ]
    }, "")

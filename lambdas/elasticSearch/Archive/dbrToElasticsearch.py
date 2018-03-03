import csv
import boto3
import os
import re

from zipfile import ZipFile
from io import BytesIO
from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

# Global Variables
esHost = "search-wwes-75dyceauwq2lk6pg3kf5w4254y.ap-southeast-2.es.amazonaws.com"
uploadToEs = True

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

    zipFile = ZipFile(bytestream, 'r')
    outFile = zipFile.read(zipFile.infolist()[0]).decode('utf-8')

    # Build Index Name
    reportMonth = re.search(".*\-(\d+-\d+)", key).group(1).replace("-", "")
    accountNumber = (re.search("(\d+)", key)).group(1)

    if 'resources' in key:
        indexName = "dbr-" + str(accountNumber) + "-resources-tags-" + str(reportMonth)
    else:
        indexName = "dbr-" + str(accountNumber) + "-base-" + str(reportMonth)

    # Remove existing index with same name (to avoid duplicate entries)
    deleteElasticsearchIndex(indexName)

    uploadActions = []

    # Parse and upload file contents
    for count, line in enumerate(outFile.splitlines(), 1):

        if count == 1:
            # Header Row: retrieve field names (need to encapsulate/decapsulate list for csv.reader to work)
            payloadKeysIn = list(csv.reader([line]))[0]
            payloadKeys = []
            for value in payloadKeysIn:
                payloadKeys.append(value)

            # Create list of index numbers that we want to force to be strings rather than numbers in Elasticsearch
            containsId = [index for index, string in enumerate(payloadKeys) if 'ID' in string or 'Id' in string]
            containsDate = [index for index, string in enumerate(payloadKeys) if 'Date' in string]
            forceIndexToString = containsId
            forceIndexToDate = containsDate

        else:
            # Report Body
            payloadValuesOut = []
            payloadValuesRaw = list(csv.reader([line]))[0]

            # If used to ensure summaries at bottom of file isn't imported
            if len(payloadValuesRaw[7]) > 1:
                # Convert integers and floats to numbers in the output JSON
                for index, value in enumerate(payloadValuesRaw):
                    try:
                        if index in forceIndexToString:
                            payloadValuesOut.append(value)
                        elif index in forceIndexToDate:
                            payloadValuesOut.append(str(value.replace(" ", "T") + 'Z'))
                        else:
                            payloadValuesOut.append(int(value))
                    except:
                        try:
                            payloadValuesOut.append(float(value))
                        except:
                            payloadValuesOut.append(value)

                # Check that the output row length equals the header row length.  If not, something when wrong and import will cause issues
                if len(payloadValuesOut) != len(payloadKeys):
                    print("Line " + str(count) + " is " + str(len(payloadValuesOut)) + ". Should be " + str(len(payloadKeys)) + "." + " -- " + str(payloadValuesOut))
                else:
                    # All good, upload to ES if > 1000 items
                    payload = dict(zip(payloadKeys, payloadValuesOut))

                    uploadActions.append({"_index": indexName, "_type": "DetailedBillingReport", "_source": payload})
                    if len(uploadActions) >= 1000:
                        uploadToElasticsearch(uploadActions)
                        uploadActions = []


    if len(uploadActions) > 0:
        uploadToElasticsearch(uploadActions)


def uploadToElasticsearch(actions):

    if uploadToEs:
        es = returnElasticsearchAuth()

        helpers.bulk(es, actions)
        print("Lines uploaded: " + str(len(actions)))
    else:
        print("Upload set to 'False'.  Would've uploaded: " + str(len(actions)) + " lines.")


def listElasticsearchIndices():
    es = returnElasticsearchAuth()
    print(es.indices.get_alias("*"))


def deleteElasticsearchIndex(indexName):
    es = returnElasticsearchAuth()
    es.indices.delete(index=indexName, ignore=[400, 404])


def returnElasticsearchAuth():

    # Retrieve Access details (Lambda IAM role must have access to ES domain to work)
    awsauth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                              aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                              aws_token=os.environ['AWS_SESSION_TOKEN'],
                              aws_host=esHost,
                              aws_region='ap-southeast-2',
                              aws_service='es')

    es = Elasticsearch(host=esHost,
                       port=80,
                       connection_class=RequestsHttpConnection,
                       http_auth=awsauth)

    return es
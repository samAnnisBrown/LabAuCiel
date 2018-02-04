import csv
import boto3
import os
import re

from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

# Global Variables
esHost = "vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com"
uploadToEs = True


def handler(event, context):

    # Retrieve S3 object from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Download S3 file
    s3 = boto3.client('s3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Open file
    outfile = s3file['Body'].read().decode('utf-8')

    # Build Index Name
    fileName = re.search(".*/(.+?)\.", "/" + key).group(1)
    indexName = ("adhoc-" + str(fileName)).lower()

    # Remove existing index with same name (to avoid duplicate entries)
    deleteElasticsearchIndex(indexName)

    uploadActions = []

    # Parse and upload file contents
    for count, line in enumerate(outfile.splitlines(), 1):

        if count == 1:
            # Header Row: retrieve field names (need to encapsulate/decapsulate list for csv.reader to work)
            payloadKeys = list(csv.reader([line]))[0]

        else:
            # Report Body
            payloadValuesOut = []
            payloadValuesRaw = list(csv.reader([line]))[0]

            # Convert integers and floats to numbers in the output JSON
            for index, value in enumerate(payloadValuesRaw):
                try:
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

                uploadActions.append({"_index": indexName, "_type": "CostReport", "_source": payload})
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

import re
import boto3
import io
import gzip
import json
import time
import os
import hashlib
import elasticsearch
from elasticsearch import helpers   # To interact with Elasticsearch

# Global Variables
totalLinesUploadedCount = 0     # Do not modify
totalLinesCount = 0             # Do not modify


def lambda_handler(event, context):
    startTime = time.time()         # Do not modify
    time.sleep(1)

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    key = key.replace('%3D', '=')

    fileName = re.search("(.+?)/", key).group(1)
    year = re.search("year.+?(\d{4})", key).group(1)
    month = re.search("month.+?(\d{2})", key).group(1)

    indexName = 'cur-' + fileName + '-' + year + month

    # Download S3 file
    s3 = getAuth('ap-southeast-2', 's3')
    print('[DOWNLOADING] - s3://' + bucket + '/' + key)
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    print('[UNZIPPING] - into memory.')
    bytestream = io.BytesIO(s3file['Body'].read())
    outfile = gzip.GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    # Prepare variables
    linesToUpload = []
    global totalLinesCount
    totalLinesCount = len(outfile.splitlines())

    # Parse and upload file contents
    for count, line in enumerate(outfile.splitlines()):
        # Convert floats to numbers in the output JSON
        payload = json.loads(line)
        payloadkeys = []
        payloadvalues = []
        for key, value in payload.items():
            key = key.replace('/', '_')
            key = key.replace(':', '_')
            payloadkeys.append(key)
            payloadvalues.append(value)

        payload = dict(zip(payloadkeys, payloadvalues))
        docId = hashlib.md5(str(payload['identity_LineItemId'] + payload['identity_TimeInterval']).encode('utf-8')).hexdigest()
        # Create the required JSON for Elasticsearch upload
        linesToUpload.append({"_index": indexName, "_id": docId, "_type": "cur_doc", "_source": json.dumps(payload)})

        # If linesToUpload is > 5000, complete a bulk upload
        if len(linesToUpload) >= 5000:
            uploadToElasticsearch(linesToUpload, indexName)
            linesToUpload = []

    # If there are any lines left once loop is completed, upload them.
    if len(linesToUpload) > 0:
        uploadToElasticsearch(linesToUpload, indexName)

    # Final Cleanups
    print('')                       # Makes it nicer when running from CLI
    global totalLinesUploadedCount
    totalLinesUploadedCount = 0     # Reset count to 0 for next invocation


# Handles the uploading of files to the Elasticsearch endpoint, and printing upload details
def uploadToElasticsearch(actions, indexName):
    global totalLinesUploadedCount

    es = returnElasticsearchAuth()
    totalLinesUploadedCount += len(actions)
    percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)

    helpers.bulk(es, actions)
    currentRunTime = time.time() - startTime
    print('* ' + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%) - runtime (" + str(round(currentRunTime, 2)) + 's)', end='\r')


# Return ES auth, depending on whether it's in a Lambda function or not
def returnElasticsearchAuth():
    esEndpoint = os.environ['esEndpoint']
    es = elasticsearch.Elasticsearch(host=esEndpoint,
                                     port=80,
                                     connection_class=elasticsearch.RequestsHttpConnection)

    return es


def getAuth(region, service):
    auth = boto3.client(service, region_name=region)
    return auth


os.environ['esEndpoint'] = 'vpc-wildwest-gw7tbux4h6vom3xqucxpqqusre.ap-southeast-2.es.amazonaws.com'

def manualLaunch():  # If not in a Lambda, launch main function and pass S3 event JSON
    lambda_handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": 'ansamual-cur-02-transformed',
                    },
                    "object": {
                        "key": 'rmit-billing-reports/year=2017/month=11/day=08/hourly-report-2.json.gz',
                    }
                }
            }
        ]
    }, None)

manualLaunch()
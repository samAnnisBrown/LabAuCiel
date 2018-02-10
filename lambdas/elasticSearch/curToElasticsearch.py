import csv
import boto3
import os
import re
import sys
import requests
import argparse

from gzip import GzipFile
from io import BytesIO
from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from time import sleep

# Argument Creation
parser = argparse.ArgumentParser(description="Testing, 123.")
parser.add_argument('--elasticsearch_endpoint',
                    default='vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com',
                    help='Defines the Elasticsearch endpoint FQDN (do not use URL)')
# Working with Indexes
parser.add_argument('--index_list', action='store_true',
                    help='Lists the indices described by the --elasticsearch_endpoint parameter.')
parser.add_argument('--index_delete',
                    help='Deletes an Elasticsearch index.  Enter the index name to delete')
# Ad-hoc uploading of CUR data
parser.add_argument('--cur_load',
                    action='store_true',
                    help='Manually load CUR data (use this parameter if script is not a Lambda function triggered by S3).  Requires --bucket and --key to be set showing the location of the CUR .csv.gz file.  Use --role_arn if using STS to assume the role in another account, otherwise standard local BOTO3 auth attempts will be used.')
parser.add_argument('--role_arn',
                    help='If using STS auth, the ARN of the role to be assumed.')
parser.add_argument('--bucket',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
parser.add_argument('--key',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
parser.add_argument('--dryrun',
                    action='store_true',
                    help='Show output of upload without impacting Elasticsearch cluster.')

args = parser.parse_args()

# Global Variables
totalLinesUploadedCount = 0  # Do not modify
totalLinesCount = 0  # Do not modify


# Lambda/Main Import Function
def lambda_handler(event, context):
    print('Running main import function')
    sleep(3)  # If lambda is in a VPC, DNS resolution isn't immediate as the ENI is attached - wait a bit just to make sure we can resolve S3 and ES

    # Retrieve S3 object from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Download S3 file
    if args.role_arn is not None:
        client = boto3.client('sts')
        assumed_role = client.assume_role(
            RoleArn=args.role_arn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']

        s3 = boto3.client('s3',
                            region_name='ap-southeast-2',
                            aws_access_key_id=creds['AccessKeyId'],
                            aws_secret_access_key=creds['SecretAccessKey'],
                            aws_session_token=creds['SessionToken'], )
    else:
        s3 = boto3.client('s3', region_name='ap-southeast-2')

    print('Downloading \"' + bucket + '/' + key + '\" from S3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    # TODO use scratch space on disk instead? Lambda has only 500Mb though :(
    print('Unzipping into memory - depending on the size of the CUR, this could take a while...')
    bytestream = BytesIO(s3file['Body'].read())
    outfile = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    # Build Index Name - if triggered from default CUR key, will include year/month - otherwise, only filename
    try:
        reportMonth = re.search(".*/(\d+-\d+)/", key).group(1).split("-")[0][:-2]
        reportName = (re.search(".*/(.+?)/\d+-\d+/", key)).group(1)
        indexName = ("cur-" + str(reportName) + "-" + str(reportMonth)).lower()
    except:
        keyName = (re.search("(.+.)csv.gz", key)).group(1)
        indexName = ("cur-adoc-" + str(keyName).lower())

    # Remove existing index with same name (to avoid duplicate entries)
    if args.dryrun is False:
        deleteElasticsearchIndex(indexName)

    # Prepare variables
    linesToUpload = []
    global totalLinesCount
    totalLinesCount = len(outfile.splitlines())

    # Parse and upload file contents
    for count, line in enumerate(outfile.splitlines(), 1):

        if count == 1:  # Header Row: retrieve field names
            payloadKeysIn = list(csv.reader([line]))[0]  # need to encapsulate/decapsulate list for csv.reader to work

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
                          or 'Amortized' in string
                          or 'Unused' in string
                          or 'EffectiveCost' in string
                          or 'TotalReserved' in string
                          or 'NormalizedUnitsPerReservation' in string
                          or 'NumberOfReservations' in string
                          or 'UnitsPerReservation' in string
                          or 'UpfrontValue' in string
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
                print("Line " + str(count) + " is " + str(len(payloadValuesOut)) + ". Should be " + str(
                    len(payloadKeys)) + "." + " -- " + str(payloadValuesOut))
            else:
                # Created the individual line payload
                payload = dict(zip(payloadKeys, payloadValuesOut))

                # Create the required JSON for Elasticsearch upload
                linesToUpload.append({"_index": indexName, "_type": "CostReport", "_source": payload})

                # If linesToUpload is > 1000, complete a bulk upload
                if len(linesToUpload) >= 1000:
                    uploadToElasticsearch(linesToUpload, indexName)
                    linesToUpload = []

    # If there are any lines left once loop is completed, upload them.
    if len(linesToUpload) > 0:
        uploadToElasticsearch(linesToUpload, indexName)


def uploadToElasticsearch(actions, indexName):
    global totalLinesUploadedCount

    if args.dryrun is False:
        es = returnElasticsearchAuth()
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)

        helpers.bulk(es, actions)
        print("Uploaded " + str(len(actions)) + " lines  - " + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%)")
    else:
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)
        print("Upload set to 'False'.  Would've uploaded " + str(len(actions)) + " lines -  " + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%)")


def listElasticsearchIndices():
    es = returnElasticsearchAuth()
    print(es.indices.get_alias("*"))


def deleteElasticsearchIndex(indexName):
    es = returnElasticsearchAuth()
    es.indices.delete(index=indexName, ignore=[400, 404])


def returnElasticsearchAuth():
    if not args.cur_load:
        # Retrieve Access details (Lambda IAM role must have access to ES domain to work)
        awsauth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                                  aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                  aws_token=os.environ['AWS_SESSION_TOKEN'],
                                  aws_host=args.elasticsearch_endpointost,
                                  aws_region='ap-southeast-2',
                                  aws_service='es')
    else:
        # If running outside of a Lambda function, retrieve creds using standard BOTO logic
        awsauth = BotoAWSRequestsAuth(aws_host=args.elasticsearch_endpoint,
                                      aws_region='ap-southeast-2',
                                      aws_service='es')

    es = Elasticsearch(host=args.elasticsearch_endpoint,
                       port=80,
                       connection_class=RequestsHttpConnection,
                       http_auth=awsauth)

    return es


# Index Functions
if args.index_list:
    response = requests.get('http://' + args.elasticsearch_endpoint + '/_cat/indices?v&pretty')
    print(response.text)
    print('Finished listing - Existing...')
    sys.exit()

if args.index_delete:
    print('Deleting index ' + args.delete_index)
    response = requests.delete('http://' + args.elasticsearch_endpoint + '/' + args.delete_index + '?pretty')
    print(response.text)
    sys.exit()

# Load Functions
# if args.cur_load:
#     if args.role_arn is not None:
#         client = boto3.client('sts')
#         assumed_role = client.assume_role(
#             RoleArn=args.role_arn,
#             RoleSessionName='tempsession'
#         )
#
#         creds = assumed_role['Credentials']
#
#         s3 = boto3.resource('s3',
#                             aws_access_key_id=creds['AccessKeyId'],
#                             aws_secret_access_key=creds['SecretAccessKey'],
#                             aws_session_token=creds['SessionToken'], )
#     else:
#         s3 = boto3.resource('s3')
#
#     bucket = s3.Bucket(name=args.bucket)
#     for s3object in bucket.objects.all():
#         print(s3object)
#
#     sys.exit()

if args.cur_load:  # If not in a Lambda, launch main function and pass S3 event JSON
    if args.bucket is None or args.key is None:
        print('Set both --bucket and --key need to location of the CUR file in S3 you want to import.')
    else:
        lambda_handler({
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": args.bucket,
                        },
                        "object": {
                            "key": args.key,
                        }
                    }
                }
            ]
        }, "")

print('End of file...')

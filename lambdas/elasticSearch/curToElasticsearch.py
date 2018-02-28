import csv          # To deal with the CUR CSVs
import boto3        # To interact with AWS
import os           # To get OS environmental variabls for auth
import re           # For all the regex
import sys          # To exit the script when things go wrong or are finished
import requests     # To interact with Elasticsearch (like curl)
import argparse     # For all the aguments
import operator     # For some sorting stuff
import hashlib      # MD5 Hashes for each line

from gzip import GzipFile       # So we can gunzip stuff
from io import BytesIO          # Stream bytes from S3
from elasticsearch import helpers   # To interact with Elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection     # To interact with Elasticseardh
from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from time import sleep

# Argument Creation
parser = argparse.ArgumentParser(description="This scrupts uploads CUR data to and manipulate Elasticsearch indices")
parser.add_argument('--elasticsearch_endpoint',
                    default='vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com',
                    help='Defines the Elasticsearch endpoint FQDN (do not use URL)')
# Working with Indexes
parser.add_argument('-l', '--index_list', action='store_true',
                    help='Lists the indices in the cluster described by the --elasticsearch_endpoint parameter.')
parser.add_argument('-d', '--index_delete',
                    help='Deletes an Elasticsearch index.  Enter the index name to delete')

# Auto uploading of CUR data for specific customers
parser.add_argument('-c', '--customer',
                    help='Customer - i.e. RMIT, Sportsbet')
parser.add_argument('-lm', '--list_months',
                    action='store_true',
                    help='Lists the available months for import in the customer\'s folder.  Use with the --customer parameter.')
parser.add_argument('-m', '--minus_month',
                    type=int,
                    default=0,
                    help='By default, the current month with be imported.  Use this flag to import previous months, which can be seen with the --list-months flag.  Integer represents number of months in the past (i.e. 1 = last month, 2 = 2 months ago, etc).  Use with the --customer parameter.')

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



try:
    if args.customer.lower() == 'rmit':
        args.role_arn = 'arn:aws:iam::182132151869:role/AWSEnterpriseSupportCURAccess'
        args.bucket = 'rmit-billing-reports'
        args.cur_load = True
        folderFilter = 'CUR/Hourly'
        customerImport = True
    elif args.customer.lower() == 'ansamual':
        args.bucket = 'ansamual-costreports'
        args.cur_load = True
        folderFilter = 'QuickSight_RedShift_CostReports'
        customerImport = True
    elif args.customer.lower() == 'sportsbet':
        args.role_arn = 'arn:aws:iam::794026524096:role/awsEnterpriseSupportCURAccess'
        args.bucket = 'sportsbet-billing-data'
        args.cur_load = True
        folderFilter = 'hourly'
        customerImport = True
    else:
        print('Customer \'' + args.customer.lower() + '\' unknown.  Exiting...')
        sys.exit()
except AttributeError:
    customerImport = False

# Global Variables
totalLinesUploadedCount = 0  # Do not modify
totalLinesCount = 0  # Do not modify


# Lambda/Main Import Function
def lambda_handler(event, context):
    print('[RUNNING] - main import function')
    sleep(3)  # If lambda is in a VPC, DNS resolution isn't immediate as the ENI is attached - wait a bit just to make sure we can resolve S3 and ES

    # Retrieve S3 object from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Download S3 file
    s3 = returnS3Auth()
    print('[DOWNLOADING] - \"' + bucket + '/' + key + '\" from S3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    # TODO use scratch space on disk instead? Lambda has only 500Mb though :(
    print('[UNZIPPING] - into memory - depending on the size of the CUR, this could take a while...')
    bytestream = BytesIO(s3file['Body'].read())
    outfile = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    # Build Index Name - if triggered from default CUR key, will include year/month - otherwise, only filename
    try:
        reportMonth = re.search(".*/(\d+-\d+)/", key).group(1).split("-")[0][:-2]
        # reportName = (re.search("(.+?)/", key)).group(1)
        reportName = bucket.lower()
        indexName = ("cur-" + str(reportName) + "-" + str(reportMonth)).lower()
    except:
        keyName = (re.search("(.+.)csv.gz", key)).group(1)
        indexName = ("cur-adoc-" + str(keyName).lower())

    # Remove existing index with same name (to avoid duplicate entries)
    if args.dryrun is False and '1.csv.gz' in key:
        print('[--DELETING--] - index ' + indexName + " to ensure there are no duplicates...")
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
                print("Line " + str(count) + " is " + str(len(payloadValuesOut)) + ". Should be " + str(len(payloadKeys)) + "." + " -- " + str(payloadValuesOut))
            else:
                # Created the individual line payload
                payload = dict(zip(payloadKeys, payloadValuesOut))
                
                # Create the required JSON for Elasticsearch upload
                lineHash = hashlib.md5(str(payload).encode('utf-8')).hexdigest()
                linesToUpload.append({"_index": indexName, "_type": "CostReport", "_id": lineHash, "_source": payload})

                # If linesToUpload is > 1000, complete a bulk upload
                if len(linesToUpload) >= 250:
                    uploadToElasticsearch(linesToUpload, indexName)
                    linesToUpload = []

    # If there are any lines left once loop is completed, upload them.
    if len(linesToUpload) > 0:
        uploadToElasticsearch(linesToUpload, indexName)

    # Final Cleanup
    print("")
    global totalLinesUploadedCount
    totalLinesUploadedCount = 0


def uploadToElasticsearch(actions, indexName):
    global totalLinesUploadedCount

    if args.dryrun is False:
        es = returnElasticsearchAuth()
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)

        helpers.bulk(es, actions)
        print('* ' + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%)", end='\r')
    else:
        totalLinesUploadedCount += len(actions)
        percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)
        print("[DRYRUN] - " + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%)", end='\r')


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


def returnS3Auth():
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

    return s3


# Index Functions
def listIndex(esEndpoint):
    response = requests.get('http://' + esEndpoint + '/_cat/indices?v&pretty')
    print(response.text)
    print('[LISTING] - Indices')
    sys.exit()


def deleteIndex(indexName, esEndpoint):
    print('[--DELETING--] - Index ' + indexName)
    response = requests.delete('http://' + esEndpoint + '/' + indexName + '?pretty')
    print(response.text)
    sys.exit()

def getLatestCurFile():
    # Create dit/list
    curFiles = {}
    outputList = []
    reportsList = []
    # Get S3 Auth
    s3 = returnS3Auth()
    # Get s3 objects and display length
    listObjectsOutput = s3.list_objects_v2(Bucket=args.bucket)
    #print('S3 objects retrieved in this call: ' + str(len(listObjectsOutput['Contents'])))
    # While the list is truncated, keep calling an appending to outputList
    while listObjectsOutput['IsTruncated']:
        outputList.append(listObjectsOutput)
        listObjectsOutput = s3.list_objects_v2(Bucket=args.bucket, ContinuationToken=listObjectsOutput['NextContinuationToken'])
        #print('S3 objects retrieved in this call: ' + str(len(listObjectsOutput['Contents'])))
    outputList.append(listObjectsOutput)

    # Let's make a new list that only contains CUR files (csv.gz)
    for listObjectsOutput in outputList:
        for s3Object in listObjectsOutput['Contents']:
            if 'csv.gz' in s3Object['Key'] and folderFilter in s3Object['Key']:
                curFiles[s3Object['Key']] = s3Object['LastModified'].isoformat()
                # Create a list of all the different folders (i.e. months) where we might want to find the 'latest'
                try:
                    searchKey = re.search("(.+/\d+-\d+)/", s3Object['Key']).group(1)
                    if searchKey not in reportsList:
                        reportsList.append(searchKey)
                except AttributeError:
                    searchKey = None

    if args.list_months:
        for report in reportsList:
            print(report)
        sys.exit()

    # Remove all keys that don't belong to the appropriate month
    keysToDelete = [k for k, v in curFiles.items() if sorted(reportsList, reverse=True)[args.minus_month] not in k]
    for key in keysToDelete:
        del curFiles[key]

    sortedCur = sorted(curFiles.items(), key=operator.itemgetter(1), reverse=True)
    #print('[CHOSE] - file "' + sortedCur[0][0] + '" with date "' + sortedCur[0][1] + '"')
    folderHash = re.search(".*/(.+-.+-.+-.+)/.+", sortedCur[0][0]).group(1)

    gzipFiles = []

    for listObjectsOutput in outputList:
        for s3Object in listObjectsOutput['Contents']:
            if 'csv.gz' in s3Object['Key'] and folderHash in s3Object['Key']:
                gzipFiles.append(s3Object['Key'])

    #if len(gzipFiles) > 1:
    print("[FOUND] - the following associated file(s) from " + sortedCur[0][1])
    for file in gzipFiles:
        print("- " + file)

    #return sortedCur[0][0], gzipFiles
    return gzipFiles


def manualCurImport(bucket, keys):
    if bucket is None or keys < 1:
        print('Set both --bucket and --key need to location of the CUR file in S3 you want to import.')
    else:
        for key in keys:
            lambda_handler({
                "Records": [
                    {
                        "s3": {
                            "bucket": {
                                "name": bucket,
                            },
                            "object": {
                                "key": key,
                            }
                        }
                    }
                ]
            }, "")


if args.index_delete:
    deleteIndex(args.index_delete, args.elasticsearch_endpoint)
if args.index_list:
    listIndex(args.elasticsearch_endpoint)
if customerImport:
    args.key = getLatestCurFile()
if args.cur_load:  # If not in a Lambda, launch main function and pass S3 event JSON
    manualCurImport(args.bucket, args.key)

print("--Finished--")
import sys
import re
import boto3
import io
import gzip
import csv
import json
import urllib
import time
import os
import elasticsearch


def lambda_handler(event, context):
    time.sleep(1)

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    fileName = re.search("(.+?)/", key).group(1)
    year = re.search("year.+?(\d{4})", key).group(1)
    month = re.search("month.+?(\d{2})", key).group(1)
    day = re.search("day.+?(\d{2})", key).group(1)


    print(year)
    print(month)
    print(day)

    indexName = 'cur-' + fileName + '-' + year + month + day
    print(indexName)

    # Download S3 file
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[DOWNLOADING] - \"' + bucket + '/' + key + '\"')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    print('[UNZIPPING] - into memory.  Depending on the size of the CUR, this could take a while...')
    bytestream = io.BytesIO(s3file['Body'].read())
    outfile = gzip.GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    # Prepare variables
    linesToUpload = []
    global totalLinesCount
    totalLinesCount = len(outfile.splitlines())

    # Parse and upload file contents
    for count, line in enumerate(outfile.splitlines(), 1):

        if count == 1:  # Header Row: retrieve field names
            payloadKeysIn = list(csv.reader([line]))[0]  # need to encapsulate/decapsulate list for csv.reader to work

            # Replace '/' and ':' with '_' for field names - these symbols clash with search syntax in Elasticsearch
            payloadKeys = []
            for value in payloadKeysIn:
                value = value.replace('/', '_')
                value = value.replace(':', '_')
                payloadKeys.append(value)

            # Columns with the below names will be numbers in Elasticsearch (so we can do math on them)
            forceFloat = ['lineItem_UsageAmount',
                          'lineItem_NormalizationFactor',
                          'lineItem_NormalizedUsageAmount',
                          'lineItem_UnblendedRate',
                          'lineItem_UnblendedCost',
                          'lineItem_BlendedRate',
                          'lineItem_BlendedCost',
                          'product_normalization',
                          'pricing_publicOnDemandCost',
                          'pricing_publicOnDemandRate',
                          'reservation_NumberOfReservations',
                          'reservation_AmortizedUpfrontCostForUsage',
                          'reservation_AmortizedUpfrontFeeForBillingPeriod',
                          'reservation_EffectiveCost',
                          'reservation_NumberOfReservations',
                          'reservation_NormalizedUnitsPerReservation',
                          'reservation_RecurringFeeForUsage',
                          'reservation_TotalReservedNormalizedUnits',
                          'reservation_TotalReservedUnits',
                          'reservation_UnitsPerReservation',
                          'reservation_UnusedAmortizedUpfrontFeeForBillingPeriod',
                          'reservation_UnusedNormalizedUnitQuantity',
                          'reservation_UnusedQuantity',
                          'reservation_UnusedRecurringFee',
                          'reservation_UpfrontValue'
                          ]
            floatIndexNumbers = []
            for i, k in enumerate(payloadKeys):
                if k in forceFloat:
                    floatIndexNumbers.append(i)
        else:   # Not header row - let's start uploading
            # Report Body
            payloadValuesOut = []
            payloadValuesRaw = list(csv.reader([line]))[0]

            # Convert floats to numbers in the output JSON
            for index, value in enumerate(payloadValuesRaw):
                if index in floatIndexNumbers:
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
                # Create the individual line payload
                payload = dict(zip(payloadKeys, payloadValuesOut))

                # Create the required JSON for Elasticsearch upload
                linesToUpload.append({"_index": indexName, "_type": "cur_doc", "_source": payload})

                # If linesToUpload is > 250, complete a bulk upload
                if len(linesToUpload) >= 1000:
                    uploadToElasticsearch(linesToUpload, indexName)
                    linesToUpload = []

    # If there are any lines left once loop is completed, upload them.
    if len(linesToUpload) > 0:
        uploadToElasticsearch(linesToUpload, indexName)

    # Final Cleanup
    print("")
    global totalLinesUploadedCount
    totalLinesUploadedCount = 0


# Handles the uploading of files to the Elasticsearch endpoint, and printing upload details
def uploadToElasticsearch(actions, indexName):
    global totalLinesUploadedCount

    es = returnElasticsearchAuth()
    totalLinesUploadedCount += len(actions)
    percent = round((totalLinesUploadedCount / totalLinesCount) * 100, 2)

    elasticsearch.helpers.bulk(es, actions)
    print('* ' + str(totalLinesUploadedCount) + " of " + str(totalLinesCount) + " lines uploaded to index " + indexName + ". (" + str(percent) + "%)", end='\r')


# Return ES auth, depending on whether it's in a Lambda function or not
def returnElasticsearchAuth():
    esEndpoint = os.environ['esEndpoint']
    es = elasticsearch.Elasticsearch(host=esEndpoint,
                       port=80,
                       connection_class=elasticsearch.RequestsHttpConnection)

    return es


def getAuth(region, service, accessType, roleArn=None):
    if roleArn is not None:
        client = boto3.client('sts')
        assumed_role = client.assume_role(
            RoleArn=roleArn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']

        if accessType == 'client':
            auth = boto3.client(service,
                                region_name=region,
                                aws_access_key_id=creds['AccessKeyId'],
                                aws_secret_access_key=creds['SecretAccessKey'],
                                aws_session_token=creds['SessionToken'])

        elif accessType == 'resource':
            auth = boto3.resource(service,
                                  region_name=region,
                                  aws_access_key_id=creds['AccessKeyId'],
                                  aws_secret_access_key=creds['SecretAccessKey'],
                                  aws_session_token=creds['SessionToken'])
    else:
        if accessType == 'client':
            auth = boto3.client(service, region_name=region)

        elif accessType == 'resource':
            auth = boto3.resource(service, region_name=region)

    return auth


os.environ['esEndpoint'] = 'http://vpc-wildwest-gw7tbux4h6vom3xqucxpqqusre.ap-southeast-2.es.amazonaws.com'

def manualLaunch():  # If not in a Lambda, launch main function and pass S3 event JSON
    lambda_handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": 'ansamual-cur-02-transformed',
                    },
                    "object": {
                        "key": 'ansamual-costreports/year=2017/month=12/day=01/quicksight_redshift_costreports-1.csv.gz',
                    }
                }
            }
        ]
    }, None)

manualLaunch()
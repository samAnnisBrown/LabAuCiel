import boto3
import re
import gzip
import os
import io
import csv
import json


def lambda_handler(event, context):
    # Generate Variables
    bucketSrc = event["Records"][0]["s3"]["bucket"]["name"]
    keySrc = event["Records"][0]["s3"]["object"]["key"]
    keySrc = keySrc.replace('%3D', '=')
    bucketDst = os.environ['bucketDst']
    fileName = re.search(".+/(.+?)\.", keySrc).group(1)
    keyPrefix = re.search("(.+?)/.*", keySrc).group(1)

    year = re.search(".+year=(\d{4})", keySrc).group(1)
    month = re.search(".+month=(\d{2})", keySrc).group(1)
    day = re.search(".+day=(\d{2})", keySrc).group(1)

    s3file = getS3FObject(bucketSrc, keySrc)

    # Unzip into memory
    print('[UNCOMPRESSING] - As bytestream...')
    bytestream = io.BytesIO(s3file['Body'].read())
    with gzip.open(bytestream, 'rt') as file:
        print('[PROCESSING] - Jsonifying CUR part.')
        filesDict = {}
        hasHeader = True
        for row in file:
            if hasHeader:
                header = buildHeaderRow(row, True)
                hasHeader = False
            else:
                csvList = list(csv.reader(row.split('\n'), quotechar='"'))[0]
                # Create a GZIP file for each day, ensuring a header row is in each
                for k, v in enumerate(csvList):
                    if v == '':
                        csvList[k] = None
                    else:
                        try:
                            csvList[k] = float(v)
                            try:
                                csvList[k] = int(v)
                            except:
                                pass
                        except:
                            pass
                payload = dict(zip(header, csvList))
                fileIndex = day
                if fileIndex not in filesDict:
                    filesDict[fileIndex] = io.BytesIO()
                    globals()['gzip_' + str(fileIndex)] = gzip.GzipFile(fileobj=filesDict[fileIndex], mode='w')
                else:
                    globals()['gzip_' + str(fileIndex)].write(str(json.dumps(payload) + '\n').encode('utf-8'))

    for k, v in filesDict.items():
        globals()['gzip_' + k].close()
        v.seek(0)
        # Put the object in S3
        uploadKey = keyPrefix + '/year=' + year + '/month=' + month + '/day=' + k + '/' + fileName + ".json.gz"
        putS3Object(bucketDst, uploadKey, v)

    deleteS3Object(bucketSrc, keySrc)


def buildHeaderRow(row, asList):
    originalList = row.rstrip().split(',')
    uniqueList = []
    for index, item in enumerate(originalList):
        if item.lower() in uniqueList:
            originalList[index] = item + str(uniqueList.count(item.lower()))
            uniqueList.append(item.lower())
        else:
            uniqueList.append(item.lower())

    if asList:
        header = originalList
    else:
        header = ','.join(originalList) + '\n'

    return header


def getS3FObject(bucket, key):
    s3 = getAuth('ap-southeast-2', 's3')
    print('[GETTING] - s3://' + bucket + '/' + key)
    s3object = s3.get_object(Bucket=bucket, Key=key)
    return s3object


def putS3Object(bucket, key, body):
    s3 = getAuth('ap-southeast-2', 's3')
    print('[PUTTING] - s3://' + bucket + '/' + key.lower())
    s3.put_object(Bucket=bucket, Key=key.lower(), Body=body)


def deleteS3Object(bucket, key):
    s3 = getAuth('ap-southeast-2', 's3')
    print('[DELETING] - s3://' + bucket + '/' + key.lower())
    s3.delete_object(Bucket=bucket, Key=key)


def getAuth(region, service):
    auth = boto3.client(service, region_name=region)

    return auth


def manualLaunch():  # If not in a Lambda, launch main function and pass S3 event JSON
    lambda_handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": 'ansamual-cur-01-sorted',
                    },
                    "object": {
                        #"key": 'ansamual-costreports/20180401/QuickSight_RedShift_CostReports-1.csv.gz',
                        #"key": 'rmit-billing-reports/20180301/Hourly-report-4.csv.gz',
                        "key": 'rmit-billing-reports/scratch/year=2018/month=03/day=13/hourly-report-4.csv.gz',
                        #"key": 'ansamual-costreports/scratch/year=2018/month=04/day=01/quicksight_redshift_costreports-1.csv.gz',
                    }
                }
            }
        ]
    }, None)

os.environ['bucketDst'] = 'ansamual-cur-02-transformed'

manualLaunch()

# rmit-billing-reports/20180301/Hourly-report-4.csv.gz
# ansamual-costreports/20180401/QuickSight_RedShift_CostReports-1.csv.gz
# ansamual-costreports/year=2018/month=04/day=01/quicksight_redshift_costreports-1.csv.gz


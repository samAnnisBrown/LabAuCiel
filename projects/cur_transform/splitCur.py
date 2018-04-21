import boto3
import re
import gzip
import os
import io


def lambda_handler(event, context):
    # Generate Variables
    bucketSrc = event["Records"][0]["s3"]["bucket"]["name"]
    keySrc = event["Records"][0]["s3"]["object"]["key"]

    # Unzip into memory
    s3file = getS3FObject(bucketSrc, keySrc)

    # Build Variables
    fileName = re.search(".+/(.+?)\.", keySrc).group(1)
    keyPrefix = re.search("(.+?)/.*", keySrc).group(1)
    keyDate = re.search(".+/(\d+)/.+", keySrc).group(1)

    print('[UNCOMPRESSING] - As bytestream...')
    bytestream = io.BytesIO(s3file['Body'].read())
    with gzip.open(bytestream, 'rt') as file:
        print('[PROCESSING] - Splitting file.')
        filesDict = {}
        hasHeader = True
        for row in file:
            if hasHeader:
                header = buildHeaderRow(row)
                hasHeader = False
            else:
                fileIndex = re.search(".+?-.+?-(\d+).+", row).group(1)   # day
                # Create a GZIP file for each day, ensuring a header row is in each
                if fileIndex not in filesDict:
                    filesDict[fileIndex] = io.BytesIO()
                    globals()['gzip_' + str(fileIndex)] = gzip.GzipFile(fileobj=filesDict[fileIndex], mode='w')
                    globals()['gzip_' + str(fileIndex)].write(header.encode('utf-8'))
                else:
                    globals()['gzip_' + str(fileIndex)].write(row.encode('utf-8'))

    for k, v in filesDict.items():
        globals()['gzip_' + k].close()
        v.seek(0)
        # Put the object in S3
        uploadKey = keyPrefix + '/scratch/year=' + keyDate[0:4] + '/month=' + keyDate[4:6] + '/day=' + k + '/' + fileName + ".tmp.gz"
        putS3Object(bucketSrc, uploadKey, v)


def buildHeaderRow(row):
    originalList = row.rstrip().split(',')
    uniqueList = []
    for index, item in enumerate(originalList):
        if item.lower() in uniqueList:
            originalList[index] = item + str(uniqueList.count(item.lower()))
            uniqueList.append(item.lower())
        else:
            uniqueList.append(item.lower())

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


def getAuth(region, service, roleArn=None):
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
                        "key": 'rmit-billing-reports/20171101/Hourly-report-3.csv.gz',
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


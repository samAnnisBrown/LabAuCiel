import boto3
import re
import gzip
import os
import io
import csv
import json
import time

startTime = time.time()         # Do not modify


def lambda_handler(event, context):
    # Generate Variables
    bucketSrc = event["Records"][0]["s3"]["bucket"]["name"]
    keySrc = event["Records"][0]["s3"]["object"]["key"]
    keySrc = keySrc.replace('%3D', '=')
    bucketDst = os.environ['bucketDst']
    fileName = re.search(".+/(.+?)\.", keySrc).group(1)
    keyPrefix = re.search("(.+?)/.*", keySrc).group(1)

    if 'scratch' not in keySrc:
        keyDate = re.search(".+/(\d+)/.+", keySrc).group(1)
        s3file = getS3FObject(bucketSrc, keySrc)

        # Unzip into memory
        print('[UNZIPPING] - into memory.')
        bytestream = io.BytesIO(s3file['Body'].read())
        dying =False
        with gzip.open(bytestream, 'rt') as file:
            print('[PROCESSING] - extracted CUR file.')
            filesDict = {}
            hasHeader = True
            for row in file:
                if hasHeader:
                    header = buildHeaderRow(row, False)
                    hasHeader = False
                else:
                    currentRunTime = time.time() - startTime
                    if currentRunTime > 245 and dying is False:
                        print('About to die: ' + keySrc + ' - daily file')
                        dying = True

                    fileIndex = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(3)   # day
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
            uploadKey = keyPrefix + '/scratch/year=' + keyDate[0:4] + '/month=' + keyDate[4:6] + '/day=' + k + '/' + fileName + ".csv.gz"
            putS3Object(bucketSrc, uploadKey, v)
    else:
        year = re.search(".+year=(\d{4})", keySrc).group(1)
        month = re.search(".+month=(\d{2})", keySrc).group(1)
        day = re.search(".+day=(\d{2})", keySrc).group(1)

        s3file = getS3FObject(bucketSrc, keySrc)
        deleteS3Object(bucketSrc, keySrc)

        # Unzip into memory
        print('[UNZIPPING] - into memory.')
        bytestream = io.BytesIO(s3file['Body'].read())
        dying =False
        with gzip.open(bytestream, 'rt') as file:
            print('[PROCESSING] - extracted CUR file.')
            filesDict = {}
            hasHeader = True
            for row in file:
                currentRunTime = time.time() - startTime
                if currentRunTime > 245 and dying is False:
                    print('About to die: ' + keySrc + ' - json file')
                    dying = True
                # Build the header row for each file
                if hasHeader:
                    header = buildHeaderRow(row, True)
                    hasHeader = False
                # Process each line to remove commas between quotes - also extract day for data partitioning
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


def replaceCommasBetweenQuotes(row):
    lineAsList = row.split('"')
    # If index odd, the item is between quotes, so replace all commas with escaped commas
    for i, part in enumerate(lineAsList):
        # Replace on odds only
        if i % 2 != 0:
            lineAsList[i] = part.replace(",", ".")
    # Rejoin line as string
    output = ''.join(lineAsList)
    print(output)

    return output


def getS3FObject(bucket, key):
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[GETTING] - s3://' + bucket + '/' + key)
    s3object = s3.get_object(Bucket=bucket, Key=key)

    return s3object


def putS3Object(bucket, key, body):
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[PUTTING] - s3://' + bucket + '/' + key.lower())
    s3.put_object(Bucket=bucket, Key=key.lower(), Body=body)


def deleteS3Object(bucket, key):
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[DELETING] - s3://' + bucket + '/' + key.lower())
    s3.delete_object(Bucket=bucket, Key=key)


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


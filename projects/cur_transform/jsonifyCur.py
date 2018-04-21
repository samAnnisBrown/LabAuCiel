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
    fileName = re.search(".+/(.+)", keySrc).group(1)
    keyPrefix = re.search("(.+?)/.*", keySrc).group(1)

    keyDate = re.search(".+/(\d+)/.+", keySrc).group(1)

    s3file = getS3FObject(bucketSrc, keySrc)

    # Unzip into memory
    print('[UNZIPPING] - into memory.')
    bytestream = io.BytesIO(s3file['Body'].read())
    with gzip.open(bytestream, 'rt') as file:
        print('[PROCESSING] - extracted CUR file.')
        filesDict = {}
        hasHeader = True
        for row in file:
            # Build the header row for each file
            if hasHeader:
                header = buildHeaderRow(row)
                hasHeader = False
            # Process each line to remove commas between quotes - also extract day for data partitioning
            else:
                day = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(3)
                new = list(csv.reader(row.split('\n'), quotechar='"'))[0]
                # Create a GZIP file for each day, ensuring a header row is in each
                fileIndex = day
                for k, v in enumerate(new):
                    if v == '':
                        new[k] = None
                    else:
                        try:
                            new[k] = float(v)
                            try:
                                new[k] = int(v)
                            except:
                                pass
                        except:
                            pass
                payload = dict(zip(header, new))
                if day not in filesDict:
                    filesDict[fileIndex] = io.BytesIO()
                    globals()['gzip_' + str(fileIndex)] = gzip.GzipFile(fileobj=filesDict[fileIndex], mode='w')
                else:
                    globals()['gzip_' + str(fileIndex)].write(str(json.dumps(payload) + '\n').encode('utf-8'))

    for k, v in filesDict.items():
        globals()['gzip_' + k].close()
        v.seek(0)
        # Put the object in S3
        uploadKey = keyPrefix + '/year=' + keyDate[0:4] + '/month=' + keyDate[4:6] + '/day=' + k + '/' + fileName
        putS3Object(bucketDst, uploadKey, v)


def buildHeaderRow(row):
    header = row.rstrip().split(',')
    uniqueList = []
    for index, item in enumerate(header):
        if item.lower() in uniqueList:
            header[index] = item + str(uniqueList.count(item.lower()))
            uniqueList.append(item.lower())
        else:
            uniqueList.append(item.lower())

    return header


def getS3FObject(bucket, key):
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[GETTING] - s3://' + bucket + '/' + key)
    s3object = s3.get_object(Bucket=bucket, Key=key)

    return s3object


def putS3Object(bucket, key, body):
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[PUTTING] - s3://' + bucket + '/' + key.lower())
    s3.put_object(Bucket=bucket, Key=key.lower(), Body=body)


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
                        "key": 'rmit-billing-reports/20180301/Hourly-report-4.csv.gz',
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


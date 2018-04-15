import boto3
import re
import gzip
import os
import io


def lambda_handler(event, context):
    # Generate Variables
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    fileName = re.search(".+/(.+)\.", key).group(1)
    keyDate = re.search(".+/(\d+)/.+", key).group(1)
    keyPrefix = re.search("(.+?)/", key).group(1)
    bucketDst = os.environ['bucketDst']

    # Download S3 file
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('[DOWNLOADING] - ' + bucket + '/' + key + ' from S3.')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    print('[UNZIPPING] - into memory.')
    bytestream = io.BytesIO(s3file['Body'].read())
    with gzip.open(bytestream, 'rt') as file:
        print('[PROCESSING] - extracted CUR file.')
        dailyFiles = {}
        for row in file:
            # Build the header row for each file
            if 'identity/LineItemId' in row:
                originalList = row.rstrip().split(',')
                uniqueList = []
                for index, item in enumerate(originalList):
                    if item.lower() in uniqueList:
                        originalList[index] = item + str(uniqueList.count(item.lower()))
                        uniqueList.append(item.lower())
                    else:
                        uniqueList.append(item.lower())

                header = ','.join(originalList) + '\n'
            # Process each line to remove commas between quotes - also extract day for data partitioning
            else:
                day = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(3)
                # Split on quotes
                lineAsList = row.split('"')
                # If index odd, the item is between quotes, so replace all commas with escaped commas
                for i, part in enumerate(lineAsList):
                    # Replace on odds only
                    if i % 2 != 0:
                        lineAsList[i] = part.replace(",", ".")
                # Rejoin line as string
                row = ''.join(lineAsList)

                # Create a GZIP file for each day, ensuring a header row is in each
                if day not in dailyFiles:
                    dailyFiles[day] = io.BytesIO()
                    globals()['gzip_' + day] = gzip.GzipFile(fileobj=dailyFiles[day], mode='w')
                    globals()['gzip_' + day].write(header.encode('utf-8'))
                else:
                    globals()['gzip_' + day].write(row.encode('utf-8'))

    for k, v in dailyFiles.items():
        globals()['gzip_' + k].close()
        v.seek(0)
        # Put the object in S3
        uploadKey = keyPrefix + '/year=' + keyDate[0:4] + '/month=' + keyDate[4:6] + '/day=' + k + '/' + fileName + '.gz'
        print('[UPLOADING] - transformed CSV to S3 - ' + uploadKey.lower())
        s3.put_object(Bucket=bucketDst, Key=uploadKey.lower(), Body=v)


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
                        "name": 'ansamual-cur-clean',
                    },
                    "object": {
                        "key": 'rmit-billing-reports/20180301/Hourly-report-4.csv.gz',
                    }
                }
            }
        ]
    }, None)

os.environ['bucketDst'] = 'ansamual-cur-transformed'

manualLaunch()

# rmit-billing-reports/20180301/Hourly-report-4.csv.gz
# ansamual-costreports/20180401/QuickSight_RedShift_CostReports-1.csv.gz


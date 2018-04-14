import boto3
import re
from io import BytesIO
import gzip
import time
import gc
import csv
import os
import psutil

process = psutil.Process(os.getpid())


def lambda_handler(event, context):
    # Generate Variables
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    fileName = re.search(".+/(.+)\.", key).group(1)
    yearMonth = re.search(".+/(\d+)/.+", key).group(1)

    print(yearMonth)
    print(fileName)

    # Complete Transform
    #print("After Unzip - " + str(process.memory_info().rss))
    #transformToS3(curCsv, fileName, yearMonth)
    #updateAthena(curCsv)


    # Download S3 file
    s3 = getAuth('ap-southeast-2', 's3', 'client')
    print('Downloading \"' + bucket + '/' + key + '\" from S3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    print('Unzipping into memory...')
    bytestream = BytesIO(s3file['Body'].read())

    with gzip.open(bytestream, 'rt') as file:
        file_buffer = BytesIO()
        with gzip.GzipFile(fileobj=file_buffer, mode='w') as f:
            for row in file.readlines()[0:1]:
                orginalList = row.split(',')
                for item in orginalList:
                    print(item)
                #f.write(str(row).encode('utf-8'))

            for row in file.readlines()[1:]:
                print(row)
                if '"' in row:
                    # Split on quotes
                    lineAsList = row.split('"')
                    # If index odd, the item is between quotes, so replace all commas with escaped commas
                    for i, part in enumerate(lineAsList):
                        # Replace on odds only
                        if i % 2 != 0:
                            lineAsList[i] = part.replace(",", ".")
                    # Rejoin line as string
                    row = ''.join(lineAsList)
                    #print(line)
                f.write(str(row).encode('utf-8'))


        # file_buffer = BytesIO()
        # with gzip.GzipFile(fileobj=file_buffer, mode='w') as f:
        #     reader = csv.reader(file, delimiter=',', quotechar='"')
        #     for row in reader:
        #         for index, item in enumerate(row):
        #             row[index] = item.replace(",", "|,")
        #         f.write(str(row).encode('utf-8'))

    file_buffer.seek(0)
    # Put the object in S3
    uploadKey = 'rmit' + '/year=' + yearMonth[0:4] + '/month=' + yearMonth[4:6] + '/' + fileName + '.gz'
    print('Uploading unzipped and transformed CSV to ' + uploadKey.lower())
    s3.put_object(Bucket='ansamual-cur-transformed', Key=uploadKey.lower(), Body=file_buffer)
    #outfile = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
    #gc.collect()


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


manualLaunch()
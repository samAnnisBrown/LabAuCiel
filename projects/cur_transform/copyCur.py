import boto3
import re
import json
import sys


def lambda_handler(event, context):
    
    # REPORT DETAILS

    bucketSrc = 'ansamual-costreports'
    prefix = 'QuickSight_RedShift'
    report = 'QuickSight_RedShift_CostReports'
    roleArn = ''

    # bucketSrc = 'rmit-billing-reports'
    # prefix = 'CUR'
    # report = 'Hourly-report'
    # roleArn = 'arn:aws:iam::182132151869:role/AWSEnterpriseSupportCURAccess'
    
    # bucketSrc = 'sportsbet-billing-data'
    # prefix = 'hourly'
    # report = 'sportsbet-hourly-cost-report'
    # roleArn = 'arn:aws:iam::794026524096:role/awsEnterpriseSupportCURAccess'

    # DESTINATION DETAILS
    region = 'ap-southeast-2'
    bucketDst = 'ansamual-cur-sorted'

    if roleArn == '':
        s3src = getS3Auth(region, 'client', None)
        s3dst = s3src
    else:
        s3src = getS3Auth(region, 'client', roleArn)
        s3dst = getS3Auth(region, 'client', None)
    
    if prefix is None:
        keyPrefix =  '/' + report + '/'
    else:
        keyPrefix = prefix + '/' + report + '/'
    
    objects = s3src.list_objects_v2(Bucket=bucketSrc,
                                 Delimiter='/',
                                 Prefix=keyPrefix)

    for prefix in objects['CommonPrefixes']:
        try:
            reportMonth = re.search(".+/(\d+)-", prefix['Prefix']).group(1)
            manifestLocation = prefix['Prefix'] + report + '-Manifest.json'
            manifestFile = s3src.get_object(Bucket=bucketSrc, Key=manifestLocation)
            manifestFileContents = manifestFile['Body'].read().decode('utf-8')
            manifestJsonContents = json.loads(manifestFileContents)

            for keySrc in manifestJsonContents['reportKeys']:
                objectName = re.search(".+/(.*)", keySrc).group(1)
                keyDst = bucketSrc + '/' + reportMonth + '/' + objectName
                
                fileData = s3src.get_object(Bucket=bucketSrc, Key=keySrc)
                s3dst.put_object(Bucket=bucketDst, Key=keyDst, Body=fileData['Body'].read())
                
                #objectName = re.search(".+/(.*)", keySrc).group(1)
                #objectSrc = { 'Bucket': bucketSrc, 'Key': keySrc }
                #objectDst = report + '/' + reportMonth + '/' + objectName
                #bucket = s3r.Bucket(bucketDst)
                print('[COPYING]\nFROM: s3://' + bucketSrc + '/' + keySrc + '\nTO: s3://' + bucketDst + '/' + keyDst)
                #bucket.copy(objectSrc, objectDst)
            
        except Exception as e:
            print(e)
            pass


def getS3Auth(region, accessType, roleArn):
    if roleArn is not None:
        client = boto3.client('sts')
        assumed_role = client.assume_role(
            RoleArn=roleArn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']
        
        if accessType == 'client':
            s3 = boto3.client('s3',
                              region_name=region,
                              aws_access_key_id=creds['AccessKeyId'],
                              aws_secret_access_key=creds['SecretAccessKey'],
                              aws_session_token=creds['SessionToken'], )
        
        elif accessType == 'resource':
            s3 = boto3.resource('s3',
                              region_name=region,
                              aws_access_key_id=creds['AccessKeyId'],
                              aws_secret_access_key=creds['SecretAccessKey'],
                              aws_session_token=creds['SessionToken'], )
        else:
            print('Didn\'t get any creds')
    else:
        if accessType == 'client':
            s3 = boto3.client('s3', region_name=region)
        
        elif accessType == 'resource':
            s3 = boto3.resource('s3', region_name=region)
        else:
            print('Didn\'t get any creds')

    return s3
    

def testing():
    bucketSrc = 'rmit-billing-reports'
    region = 'ap-southeast-2'
    roleArn = 'arn:aws:iam::182132151869:role/AWSEnterpriseSupportCURAccess'
    
    s3c = getS3Auth(region, 'client', roleArn)

    
    objects = s3c.list_objects_v2(Bucket=bucketSrc)
    for key in objects['Contents']:
        print(key)
    
#testing()

lambda_handler(None, None)
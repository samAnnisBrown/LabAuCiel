import boto3
import re
import json


def lambda_handler(event, context):
    
    # VARIABLES
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
    
    if prefix == '':
        keyPrefix = '/' + report + '/'
    else:
        keyPrefix = prefix + '/' + report + '/'
    
    objects = s3src.list_objects_v2(Bucket=bucketSrc,
                                 Delimiter='/',
                                 Prefix=keyPrefix)

    for prefix in objects['CommonPrefixes']:
        try:
            reportMonth = re.search(".+/(\d+)-", prefix['Prefix']).group(1)
            # Load the month's manifest file to retrieve the latest file locations
            manifestLocation = prefix['Prefix'] + report + '-Manifest.json'
            manifestFile = s3src.get_object(Bucket=bucketSrc, Key=manifestLocation)
            manifestFileContents = manifestFile['Body'].read().decode('utf-8')
            manifestJsonContents = json.loads(manifestFileContents)

            for keySrc in manifestJsonContents['reportKeys']:
                objectName = re.search(".+/(.*)", keySrc).group(1)
                keyDst = bucketSrc + '/' + reportMonth + '/' + objectName
                
                fileData = s3src.get_object(Bucket=bucketSrc, Key=keySrc)
                s3dst.put_object(Bucket=bucketDst, Key=keyDst, Body=fileData['Body'].read())
                
                print('[COPYING] - FROM: s3://' + bucketSrc + 'TO: s3://' + bucketDst + '/' + keyDst)
        except:
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
    

lambda_handler(None, None)
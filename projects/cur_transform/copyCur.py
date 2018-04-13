import boto3
import re
import json


def lambda_handler(event, context):

    bucketSrc = 'ansamual-costreports'
    bucketDst = 'ansamual-cur-sorted'
    prefix = 'QuickSight_RedShift'
    report = 'QuickSight_RedShift_CostReports'
    region = 'ap-southeast-2'
    roleArn = ''

    s3c = getS3Auth(region, 'client')
    s3r = getS3Auth(region, 'resource')
    
    objects = s3c.list_objects_v2(Bucket=bucketSrc,
                                 Delimiter='/',
                                 Prefix=prefix + '/' + report + '/')

    for prefix in objects['CommonPrefixes']:
        try:
            reportMonth = re.search(".+/(\d+)-", prefix['Prefix']).group(1)
            manifestLocation = prefix['Prefix'] + report + '-Manifest.json'
            manifestFile = s3c.get_object(Bucket=bucketSrc, Key=manifestLocation)
            manifestFileContents = manifestFile['Body'].read().decode('utf-8')
            manifestJsonContents = json.loads(manifestFileContents)

            for keySrc in manifestJsonContents['reportKeys']:
                objectName = re.search(".+/(.*)", keySrc).group(1)
                objectSrc = { 'Bucket': bucketSrc, 'Key': keySrc }
                objectDst = report + '/' + reportMonth + '/' + objectName
                bucket = s3r.Bucket(bucketDst)
                print('[COPYING]\nFROM: s3://' + bucketSrc + '/' + manifestLocation + '\nTO: s3://' + bucketDst + '/' + objectDst)
                bucket.copy(objectSrc, objectDst)
            
        except Exception as e:
            pass


def getS3Auth(region, accessType, roleArn):
    if roleArn is not None:
        client = boto3.client('sts')
        assumed_role = client.assume_role(
            RoleArn=roleArn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']
        print(creds)
        
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
    
testing()

#lambda_handler(None, None)
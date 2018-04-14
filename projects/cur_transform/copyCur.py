import boto3
import re
import json
import os


# This function should always be run from the same account that contains the dst bucket
# If the src bucket is in different account, provide the ARN of a role that has permission to assume role into the src account and retreive the CUR files
def lambda_handler(event, context):

    # Retrieve Variables from Environment
    bucketSrc = os.environ['bucketSrc']
    bucketDst = os.environ['bucketDst']
    report = os.environ['report']
    prefix = os.environ['prefix']
    region = os.environ['region']
    roleArn = os.environ['roleArn']

    # If roleArn is set, but blank, then assume src and dst buckets are in the same account, an retrieve auth appropriately
    if roleArn == '':
        s3ClientSrc = getAuth(region, 's3', 'client')
        s3ClientDst = s3ClientSrc
    # Else us STS to access bucket in src account.
    else:
        s3ClientSrc = getAuth(region, 's3', 'client', roleArn)
        s3ClientDst = getAuth(region, 's3', 'client')

    # Build the key prefix based on variables provided.
    keyPrefix = '/' + report + '/'
    if prefix != '':
        keyPrefix = prefix + keyPrefix

    # Get a list of the report months available.
    objects = s3ClientSrc.list_objects_v2(Bucket=bucketSrc,
                                          Delimiter='/',
                                          Prefix=keyPrefix)

    for prefix in objects['CommonPrefixes']:
        try:
            reportMonth = re.search(".+/(\d+)-", prefix['Prefix']).group(1)
            # Retrieve and load the month's manifest file so we can get the latest file locations.
            manifestLocation = prefix['Prefix'] + report + '-Manifest.json'
            manifestFile = s3ClientSrc.get_object(Bucket=bucketSrc, Key=manifestLocation)
            manifestFileContents = manifestFile['Body'].read().decode('utf-8')
            manifestJsonContents = json.loads(manifestFileContents)

            # For each file in the manifest...
            for keySrc in manifestJsonContents['reportKeys']:
                # Build some variables.
                objectName = re.search(".+/(.*)", keySrc).group(1)
                keyDst = bucketSrc + '/' + reportMonth + '/' + objectName
                # Save us some $$$ by checking if the file's already been copied.
                lengthSrc = s3ClientSrc.head_object(Bucket=bucketSrc, Key=keySrc)['ResponseMetadata']['HTTPHeaders']['content-length']
                try:
                    lengthDst = s3ClientDst.head_object(Bucket=bucketDst, Key=keyDst)['ResponseMetadata']['HTTPHeaders']['content-length']
                except:
                    lengthDst = 0
                # Copy the files if there's a change in content length.
                if lengthSrc != lengthDst:
                    print('[COPYING] - FROM: s3://' + bucketSrc + ' --> TO: s3://' + bucketDst + '/' + keyDst)
                    fileData = s3ClientSrc.get_object(Bucket=bucketSrc, Key=keySrc)
                    s3ClientDst.put_object(Bucket=bucketDst, Key=keyDst, Body=fileData['Body'].read())
                # Otherwise skip the file.
                else:
                    print('[SKIPPING] - s3://' + bucketDst + '/' + keyDst + ' -- the destination file exists and has the same content length as the source.')

        except Exception as e:
            #print(e)
            pass


# Generic S3 auth function - return client or resource auth using either BOTO3 logic, or assume-role if ARN supplied.
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

# ANSAMUAL
# os.environ['bucketSrc'] = 'ansamual-costreports'
# os.environ['prefix'] = 'QuickSight_RedShift'
# os.environ['report'] = 'QuickSight_RedShift_CostReports'
# os.environ['roleArn'] = ''

# RMIT
os.environ['bucketSrc'] = 'rmit-billing-reports'
os.environ['prefix'] = 'CUR'
os.environ['report'] = 'Hourly-report'
os.environ['roleArn'] = 'arn:aws:iam::182132151869:role/AWSEnterpriseSupportCURAccess'

# Sportsbet
# os.environ['bucketSrc'] = 'sportsbet-billing-data'
# os.environ['prefix'] = 'hourly'
# os.environ['report'] = 'sportsbet-hourly-cost-report'
# os.environ['roleArn'] = 'arn:aws:iam::794026524096:role/awsEnterpriseSupportCURAccess'

os.environ['region'] = 'ap-southeast-2'
os.environ['bucketDst'] = 'ansamual-cur-clean'

lambda_handler(None, None)


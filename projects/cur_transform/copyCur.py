import boto3
import re
import json
import os

# This function must be run from the account that contains the dst bucket
# If the src bucket is in different account, provide the ARN of a role that has permission to assume role into the src account and retreive the CUR files
def lambda_handler(event, context):

    # Retrieve Variables from Environment
    try:
        bucketSrc = os.environ['bucketSrc']
        bucketDst = os.environ['bucketDst']
        report = os.environ['report']
        prefix = os.environ['prefix']
        region = os.environ['region']
        roleArn = os.environ['roleArn']
    except:
        pass

    # If roleArn is not set, use local account auth.
    if roleArn not in locals():
        s3ClientSrc = s3ClientDst = getClientAuth(region, 's3')
    # Else us STS to access bucket in src account.
    else:
        s3ClientSrc = getClientAuth(region, 's3', roleArn)
        s3ClientDst = getClientAuth(region, 's3')

    # Build the key prefix.
    keyPrefix = '/' + report + '/'
    if prefix != '':
        keyPrefix = prefix + keyPrefix

    # Get a list of the report months available.
    objects = s3ClientSrc.list_objects_v2(Bucket=bucketSrc,
                                          Delimiter='/',
                                          Prefix=keyPrefix)

    for prefix in objects['CommonPrefixes']:
        try:
            reportMonth = re.search(".+/(\d+)-", prefix['Prefix']).group(1)                                             # Regex to get month of CUR
            manifestFile = s3ClientSrc.get_object(Bucket=bucketSrc, Key=prefix['Prefix'] + report + '-Manifest.json')   # Get Manifest File
            manifestJsonContents = json.loads(manifestFile['Body'].read().decode('utf-8'))                              # Read Manifest File Contents

            # For each file in the manifest...
            for keySrc in manifestJsonContents['reportKeys']:
                # Build some variables.
                fileName = re.search(".+/(.*)", keySrc).group(1)
                keyDst = bucketSrc + '/' + reportMonth + '/' + fileName 
                # Check if the file's already been copied.
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
def getClientAuth(region, service, roleArn=None):
    if roleArn is not None:
        client = boto3.client('sts')
        assumed_role = client.assume_role(
            RoleArn=roleArn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']

        auth = boto3.client(service,
                            region_name=region,
                            aws_access_key_id=creds['AccessKeyId'],
                            aws_secret_access_key=creds['SecretAccessKey'],
                            aws_session_token=creds['SessionToken'])
        
    else:
        auth = boto3.client(service, region_name=region)

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


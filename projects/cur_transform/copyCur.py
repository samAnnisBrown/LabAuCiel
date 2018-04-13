import boto3
import re


def lambda_handler(event, context):

    bucketSrc = 'ansamual-costreports'
    bucketDst = 'ansamual-cur-clean'
    prefix = 'QuickSight_RedShift'
    report = 'QuickSight_RedShift_CostReports'
    region = 'ap-southeast-2'
    # Retrieve S3 object from event
    #bucket = event["Records"][0]["s3"]["bucket"]["name"]
    #key = event["Records"][0]["s3"]["object"]["key"]

    s3 = boto3.client('s3', region_name=region)
    objects = s3.list_objects_v2(Bucket=bucketSrc,
                                 Delimiter='/',
                                 Prefix=prefix + '/' + report + '/')

    for prefix in objects['CommonPrefixes']:
        try:
            searchKey = re.search(".+/(\d+)-", prefix['Prefix']).group(1)
            print(searchKey)
        except Exception as e:
            pass


lambda_handler(None, None)
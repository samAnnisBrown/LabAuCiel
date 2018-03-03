import boto3
import re
import os
from gzip import GzipFile       # So we can gunzip stuff
from io import BytesIO          # Stream bytes from S3
from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
from time import sleep

# Elasticsearch host name
esHost = "vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com"
indexName = 'elb-accesslogs'
esBulkUrl = "https://" + esHost + "/_bulk"


def lambda_handler(event, context):
    sleep(1)
    # Retrieve S3 Bucket/Key from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Retrieve S3 Object
    s3 = boto3.client("s3")
    s3file = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    if '.log.gz' in key:    # ALB or NLB
        print('ALB Log File')
        bytestream = BytesIO(s3file['Body'].read())
        body = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        columns = ["type",
                   "timestamp",
                   "elb",
                   "client_ip",
                   "client_port",
                   "backend_ip",
                   "backend_port",
                   "request_processing_time",
                   "backend_processing_time",
                   "response_processing_time",
                   "elb_status_code",
                   "backend_status_code",
                   "received_bytes",
                   "sent_bytes",
                   "request_method",
                   "request_url",
                   "request_version",
                   "user_agent",
                   "ssl_cipher",
                   "ssl_version",
                   "additional_info"
                   ]
        albRegex = '^(.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+):(\d+) (.[^ ]+):(\d+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (\d+) (\d+) "(\w+) (.[^ ]+) (.[^ ]+)" "(.+?)"(.[^ ]+) (.[^ ]+) (.+)\n'
        regex = re.compile(albRegex)
    else:
        print('CLB Log File')
        body = s3file['Body'].read()
        columns = ["timestamp",
                   "elb",
                   "client_ip",
                   "client_port",
                   "backend_ip",
                   "backend_port",
                   "request_processing_time",
                   "backend_processing_time",
                   "response_processing_time",
                   "elb_status_code",
                   "backend_status_code",
                   "received_bytes",
                   "sent_bytes",
                   "request_method",
                   "request_url",
                   "request_version",
                   "user_agent",
                   "ssl_cipher",
                   "ssl_version"
                   ]
        clbRegex = '^(.[^ ]+) (.[^ ]+) (.[^ ]+):(\d+) (.[^ ]+):(\d+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (\d+) (\d+) "(\w+) (.[^ ]+) (.[^ ]+)" "(.+?)"(.[^ ]+) (.[^ ]+)\n'
        regex = re.compile(clbRegex)

    # Retrieve Access details (Lambda IAM role must have access to ES domain to work)
    awsauth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                              aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                              aws_token=os.environ['AWS_SESSION_TOKEN'],
                              aws_host=esHost,
                              aws_region='ap-southeast-2',
                              aws_service='es')

    es = Elasticsearch(host=esHost,
                       port=80,
                       connection_class=RequestsHttpConnection,
                       http_auth=awsauth)

    actions = []

    # For each line in the loaded S3 file
    for line in body.strip().split("\n"):
        # Match the Regex to the line, outputting an array
        regexMatch = regex.match(line)
        if not regexMatch:
            continue

        valuesList = regexMatch.groups(0)

        # Merge column title list 'albkeys' and 'valuesList' list into dictionary
        payload = dict(zip(columns, valuesList))
        actions.append({"_index": indexName, "_type": 'doc', "_source": payload})

        # If greater than 1000 actions, commit bulk upload
        if len(actions) > 250:
            helpers.bulk(es, actions)
            actions = []
            print('Uploading 250 lines to ' + indexName)

    # When loop ends, upload whatever actions remain in final bulk upload
    if len(actions) > 0:
        helpers.bulk(es, actions)
        print('Finished uploading.')


lambda_handler({
    "Records": [
        {
            "s3": {
                "bucket": {
                    "name": "ansamual-elblogs"
                },
                "object": {
                    "key": "AWSLogs/618252783261/elasticloadbalancing/ap-southeast-2/2018/03/03/618252783261_elasticloadbalancing_ap-southeast-2_app.es-alb.8ed53396a9cf8e7c_20180303T0125Z_52.64.124.41_733847om.log.gz"
                }
            }
        }
    ]
    }, '')


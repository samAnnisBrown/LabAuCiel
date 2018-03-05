import boto3
import re
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

# Elasticsearch host name
esHost = "vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com"

albKeys = ["type", "timestamp", "elb", "client_ip", "client_port", "backend_ip", "backend_port", "request_processing_time", "backend_processing_time", "response_processing_time", "elb_status_code", "backend_status_code", "received_bytes", "sent_bytes", "request_method", "request_url", "request_version", "user_agent"]
albRegex = '^(.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+):(\\d+) (.[^ ]+):(\\d+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (\\d+) (\\d+) \"(\\w+) (.[^ ]+) (.[^ ]+)\" \"(.+)\"'

regex = re.compile(albRegex)
indexName = 'elb-accesslogs'
esBulkUrl = "https://" + esHost + "/_bulk"


def lambda_handler(event, context):
    # Retrieve S3 Bucket/Key from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Retrieve S3 Object
    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    # Read S3 Object
    body = obj["Body"].read()

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
        payload = dict(zip(albKeys, valuesList))
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

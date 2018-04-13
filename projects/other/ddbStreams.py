import os
import ast
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from aws_requests_auth.aws_auth import AWSRequestsAuth

# Elasticsearch host name
esHost = "search-wildwest-elasticsearch-3odybwxjscnkxgvkdtoz4hdc2y.ap-southeast-2.es.amazonaws.com"
indexPrefix = "ddbstream"

indexName = indexPrefix + "-" + datetime.strftime(datetime.now(), "%Y%m%d")
esBulkUrl = "http://" + esHost + "/_bulk"


def lambda_handler(event, context):

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
    payload = {}

    event = ast.literal_eval(str(event))

    for key, value in event['Records'][0]['dynamodb']['NewImage'].items():
        for subKey, subValue in value.items():
            payload[key] = subValue

    # For each line in the loaded S3 file
    actions.append({"_index": indexName, "_type": "ddbstream", "_source": payload})
    print(actions)

    # Upload to ES
    helpers.bulk(es, actions)

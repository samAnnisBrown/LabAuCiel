import boto3
import re
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Elasticsearch host name
esHost = "search-wildwest-elasticsearch-3odybwxjscnkxgvkdtoz4hdc2y.ap-southeast-2.es.amazonaws.com"
indexPrefix = "alblogs1"

# ELB access log format keys
albKeys = ["type_n", "timestamp_n", "elb_n", "client_ip_n", "client_port_n", "backend_ip_n", "backend_port_n", "request_processing_time_n", "backend_processing_time_n", "response_processing_time_n", "elb_status_code_n", "backend_status_code_n", "received_bytes_n", "sent_bytes_n", "request_method_n", "request_url_n", "request_version_n", "user_agent"]
albRegex = '^(.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+):(\\d+) (.[^ ]+):(\\d+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (\\d+) (\\d+) \"(\\w+) (.[^ ]+) (.[^ ]+)\" \"(.+)\"'


regex = re.compile(albRegex)
indexName = indexPrefix + "-" + datetime.strftime(datetime.now(), "%Y%m%d")
uploadUrl = "http://" + esHost + "/_bulk"


def lambda_handler(event, context):
    #bucket = event["Records"][0]["s3"]["bucket"]["name"]
    #key = event["Records"][0]["s3"]["object"]["key"]

    bucket = "ansamul-alb-dump"
    key = "051147082346_elasticloadbalancing_ap-southeast-2_app.www-alb-prod.f1375eb5ae1a0557_20171127T0310Z_13.55.239.31_2h6cfpna.log"

    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    body = obj["Body"].read().decode("utf-8")

    esConnect = Elasticsearch(host=esHost, port=80)
    actions = []

    for line in body.strip().split("\n"):
        match = regex.match(line)
        if not match:
            continue

        inValues = match.groups(0)
        outValues = []
        elb_name = inValues[2]

        print("IN VALUES: " + str(inValues))

        for i in range(inValues.__len__()):

            try:
                outValues.append(int(inValues[i]))
            except:
                try:
                    outValues.append(float(inValues[i]))
                except:
                    outValues.append(str(inValues[i]))


        print("OUT VALUES: " + str(outValues))

        payload = dict(zip(albKeys, outValues))

        print("PAYLOAD: " + str(payload))

        actions.append({"_index": indexName, "_type": elb_name, "_source": payload})

        print("ACTIONS: " + str(actions))
        print(len(actions))
        return

        if len(actions) > 1000:
            helpers.bulk(esConnect, actions)
            actions = []

    if len(actions) > 0:
        helpers.bulk(esConnect, actions)


lambda_handler("test", "test")
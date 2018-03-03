import requests
import sys
import json
import urllib.request
from time import sleep

#es = 'https://vpc-sydney-summit-2018-ukfuj6urblh2nzu3revuq43dze.us-east-1.es.amazonaws.com'
es = 'https://es.annisbrown.com'


def lambda_handler(event, context):
    sleep(0)                    # For ENI attach
    try:
        rt = getJsonRoot(event)     # Get root level of Json only
        getJsonLevels(event, rt)    # Get Json lower levels and merge with root
        # Send Full event to ES
        event['ElasticSearchUpload'] = 'Success'
        ToEs(event)
        return event    # For Step Function Pass-thru
    except:
        failed = event['ElasticSearchUpload'] = 'Failed'
        return failed   # For Step Function Pass-thru


def getJsonRoot(jsn):
    out = {}
    for i in jsn.items():
        if type(i[1]) is str:
            out[i[0]] = i[1]
    return out


def getJsonLevels(jsn, rt):
    for x in jsn.items():
        k = jsn[x[0]]
        vt = type(x[1])
        if vt is dict:
            getJsonLevels(k, rt)
            ToEs(mergeJson(rt, k))
        elif vt is list:
            for z in k[:]:
                getJsonLevels(z, rt)
                ToEs(mergeJson(rt, z))


def mergeJson(a, b):
    for item in a.items():
        b[item[0]] = item[1]
    return b


def ToEs(doc):
    i = 'phd-events'
    jv = json.dumps(doc).encode('utf8')
    rq = urllib.request.Request(es + '/' + i + '/doc', jv, {'Content-Type': 'application/json'}, method='POST')
    f = urllib.request.urlopen(rq)
    rsp = f.read()
    f.close()
    print(rsp)

import json
import urllib.request


def getLatestPhdEvent():
    # Variables
    #es = 'https://vpc-sydney-summit-2018-ukfuj6urblh2nzu3revuq43dze.us-east-1.es.amazonaws.com"        # Works from Lambda in us-east-1 default VPC
    es = 'https://es.annisbrown.com'                                                                    # Works from ANT (IP Whitelisted)
    index = 'phd-events'
    query = {
        "query": {
            "query_string": {
                "default_field": "ElasticSearchUpload",
                "query": "Success"
            }
        },
        "size": 1,
        "sort": [
            {
                "PhdEventTime": {
                    "order": "desc"
                }
            }
        ]
    }
    # Elasticsearch Request/Response
    payload = json.dumps(query).encode('utf-8')         # Encode query for HTTP request
    request = urllib.request.Request(es + '/' + index + '/_search', payload, {'Content-Type': 'application/json'}, method='GET')    # Build HTTP request
    response = urllib.request.urlopen(request).read()   # Send Request
    response = json.loads(response.decode('utf-8'))     # Decode response and convert to JSON

    return response['hits']['hits'][0]['_source']       # Return query payload

print(getLatestPhdEvent())
sys.exit()

def listIndex():
    response = requests.get(es + '/_cat/indices?v&pretty')
    print(response.text)
    print('Finished listing - Existing...')
    sys.exit()
#listIndex()


def deleteIndex():
    itd = ['phd-*']
    for x in itd:
        response = requests.delete(es + '/' + x + '?pretty')
        print(response.text)
    sys.exit()
#deleteIndex()


lambda_handler({
    "AvailabilityZone": "us-east-1d",
    "CreateTime": "26 Feb 2018 12:23:49",
    "Encrypted": "false",
    "Size": 20,
    "SnapshotId": "snap-0a204661289b541e0",
    "State": "in-use",
    "VolumeId": "vol-0fa020a7106de56b1",
    "VolumeType": "standard",
    "Attachment": {
        "AttachTime": "26 Feb 2018 12:23:49",
        "Device": "/dev/xvda",
        "InstanceId": "i-0a27db4e6eb409ae7",
        "State": "attached",
        "VolumeId": "vol-0fa020a7106de56b1",
        "DeleteOnTermination": "false"
    },
    "PhdEventTime": "2018-02-26T13:07:36.026000+00:00",
    "PhdEventId": "b141e3d0-b1ce-37c6-2b63-2ac4a53a39db",
    "ResourceStack": {
        "StackName": "DemoApp01",
        "StackStatus": "UPDATE_COMPLETE",
        "StackEvents": [
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "115e7480-1af6-11e8-8743-500c3d4416c5",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-26T13:07:59.026000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-3f3b85d2-aa52-4a25-90ca-f69393d3d497",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-0a27db4e6eb409ae7",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-26T13:07:58.594000+00:00",
                "ResourceStatus": "DELETE_COMPLETE"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-3a9c4429-82ba-447f-aa67-3c0ea330ad60",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-0a27db4e6eb409ae7",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-26T13:06:50.777000+00:00",
                "ResourceStatus": "DELETE_IN_PROGRESS"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "e7ba5c20-1af5-11e8-9806-50a686e4bbe6",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-26T13:06:49.171000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EIPAssoc0-UPDATE_COMPLETE-2018-02-26T13:06:46.678Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EIPAssoc0",
                "PhysicalResourceId": "eipassoc-3b92b182",
                "ResourceType": "AWS::EC2::EIPAssociation",
                "Timestamp": "2018-02-26T13:06:46.678000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE",
                "ResourceProperties": "{\"InstanceId\":\"i-0482c543699712c84\",\"AllocationId\":\"eipalloc-1e3c6328\"}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EIPAssoc0-UPDATE_IN_PROGRESS-2018-02-26T13:06:13.492Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EIPAssoc0",
                "PhysicalResourceId": "eipassoc-a381a21a",
                "ResourceType": "AWS::EC2::EIPAssociation",
                "Timestamp": "2018-02-26T13:06:13.492000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceProperties": "{\"InstanceId\":\"i-0482c543699712c84\",\"AllocationId\":\"eipalloc-1e3c6328\"}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_COMPLETE-2018-02-26T13:06:10.169Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-0482c543699712c84",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-26T13:06:10.169000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-42dc353f\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_IN_PROGRESS-2018-02-26T13:05:36.524Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-0482c543699712c84",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-26T13:05:36.524000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "Resource creation Initiated",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-42dc353f\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_IN_PROGRESS-2018-02-26T13:05:34.847Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-0a27db4e6eb409ae7",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-26T13:05:34.847000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "Requested update requires the creation of a new physical resource; hence creating one.",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-42dc353f\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "b6cfedf0-1af5-11e8-9ebd-500c286374d1",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-26T13:05:27.042000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "User Initiated"
            }
        ]
    },
    "RestoredResources": {
        "RestoreSnapshotId": "snap-00f3d5c47f34cb313",
        "RestoreImageId": "ami-42dc353f",
        "ReplacementInstance": "i-0482c543699712c84",
        "RestoredVolumes": [
            {
                "Attachments": [
                    {
                        "AttachTime": "2018-02-26T13:05:37+00:00",
                        "Device": "/dev/xvda",
                        "InstanceId": "i-0482c543699712c84",
                        "State": "attached",
                        "VolumeId": "vol-07b041a1d34abd4c1",
                        "DeleteOnTermination": "false"
                    }
                ],
                "AvailabilityZone": "us-east-1d",
                "CreateTime": "2018-02-26T13:05:37.212000+00:00",
                "Encrypted": "false",
                "Size": 20,
                "SnapshotId": "snap-00f3d5c47f34cb313",
                "State": "in-use",
                "VolumeId": "vol-07b041a1d34abd4c1",
                "VolumeType": "standard"
            }
        ]
    }
    }, '')


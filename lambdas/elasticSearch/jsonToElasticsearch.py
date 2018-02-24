import requests
import sys
import json
import urllib.request

es = 'https://search-syd-summit-2018-pdktt2gfyw7cbrtcr24rzieoxe.us-east-1.es.amazonaws.com'
i = 'phd-events'


def lmbd(event):
    sub(event)      # Sub
    rt(event)       # Root
    return event    # For Notifcation


def rt(jsn):
    lst = []
    for w in jsn.items():
        if type(w[1]) is dict:
            lst.append(w[0])
    for l in lst:
        jsn.pop(l)
    ToEs(jsn)


def sub(jsn):
    for x in jsn.items():
        k = jsn[x[0]]
        vt = type(x[1])
        if vt is dict:
            try:
                sub(k)
                lvl(k)
            except:
                pass
        elif vt is list:
            try:
                for z in k[:]:
                    sub(z)
                    lvl(z)
                    ToEs(z)
            except:
                pass


def lvl(jsn):
    for y in jsn.items():
        if type(y[1]) is list:
            del jsn[y[0]]
            ToEs(jsn)


def ToEs(doc):
    print(doc)
    return
    jv = json.dumps(doc).encode('utf8')
    rq = urllib.request.Request(es + '/' + i + '/doc', jv, {'Content-Type': 'application/json'}, method='POST')
    f = urllib.request.urlopen(rq)
    rsp = f.read()
    f.close()
    print(rsp)


def listIndex():
    response = requests.get('http://' + es[8:] + '/_cat/indices?v&pretty')
    print(response.text)
    print('Finished listing - Existing...')
    sys.exit()
#listIndex()


def deleteIndex():
    itd = ['phd-events']
    for x in itd:
        response = requests.delete('http://' + es[8:] + '/' + x + '?pretty')
        print(response.text)
    sys.exit()
#deleteIndex()

lmbd({
    "AttachTime": "2018-02-22T13:34:16.000Z",
    "Device": "/dev/xvda",
    "InstanceId": "i-09b690d965c936370",
    "State": "in-use",
    "VolumeId": "vol-0a9ef3427f6af0241",
    "DeleteOnTermination": "true",
    "AvailabilityZone": "us-east-1d",
    "CreateTime": "2018-02-22T13:34:15.957Z",
    "Encrypted": "false",
    "Size": 20,
    "SnapshotId": "snap-00ef4561bf667ec79",
    "Tags": [],
    "VolumeType": "standard",
    "ResourceStack": {
        "StackName": "DemoApp01",
        "StackStatus": "UPDATE_COMPLETE",
        "StackEvents": [
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "7f0132d0-1852-11e8-9bd0-5044763dbb7b",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-23T04:32:03.177000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-7ff618bc-3919-4910-a1ad-f863f76763a1",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-09b690d965c936370",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-23T04:32:02.675000+00:00",
                "ResourceStatus": "DELETE_COMPLETE"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-5e67dd74-1b02-4d5f-ad1c-4085f738af44",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-09b690d965c936370",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-23T04:31:15.549000+00:00",
                "ResourceStatus": "DELETE_IN_PROGRESS"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "618b3ca0-1852-11e8-ba5e-50fae97e0835",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-23T04:31:13.743000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EIPAssoc0-UPDATE_COMPLETE-2018-02-23T04:31:11.208Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EIPAssoc0",
                "PhysicalResourceId": "eipassoc-54ab8eed",
                "ResourceType": "AWS::EC2::EIPAssociation",
                "Timestamp": "2018-02-23T04:31:11.208000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE",
                "ResourceProperties": "{\"InstanceId\":\"i-058e15c2cb5a0efeb\",\"AllocationId\":\"eipalloc-1e3c6328\"}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EIPAssoc0-UPDATE_IN_PROGRESS-2018-02-23T04:30:38.415Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EIPAssoc0",
                "PhysicalResourceId": "eipassoc-a4560c1d",
                "ResourceType": "AWS::EC2::EIPAssociation",
                "Timestamp": "2018-02-23T04:30:38.415000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceProperties": "{\"InstanceId\":\"i-058e15c2cb5a0efeb\",\"AllocationId\":\"eipalloc-1e3c6328\"}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_COMPLETE-2018-02-23T04:30:35.736Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-058e15c2cb5a0efeb",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-23T04:30:35.736000+00:00",
                "ResourceStatus": "UPDATE_COMPLETE",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-b45cbfc9\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_IN_PROGRESS-2018-02-23T04:30:02.504Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-058e15c2cb5a0efeb",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-23T04:30:02.504000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "Resource creation Initiated",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-b45cbfc9\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "EC2Instance-UPDATE_IN_PROGRESS-2018-02-23T04:30:00.901Z",
                "StackName": "DemoApp01",
                "LogicalResourceId": "EC2Instance",
                "PhysicalResourceId": "i-09b690d965c936370",
                "ResourceType": "AWS::EC2::Instance",
                "Timestamp": "2018-02-23T04:30:00.901000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "Requested update requires the creation of a new physical resource; hence creating one.",
                "ResourceProperties": "{\"KeyName\":\"demoenv-us-east-1\",\"ImageId\":\"ami-b45cbfc9\",\"InstanceType\":\"t2.small\",\"Tags\":[{\"Value\":\"DemoApp01\",\"Key\":\"Name\"}]}"
            },
            {
                "StackId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "EventId": "3063c570-1852-11e8-b821-500c28b23699",
                "StackName": "DemoApp01",
                "LogicalResourceId": "DemoApp01",
                "PhysicalResourceId": "arn:aws:cloudformation:us-east-1:022787131977:stack/DemoApp01/a686c400-16bd-11e8-bec1-503aca4a58fd",
                "ResourceType": "AWS::CloudFormation::Stack",
                "Timestamp": "2018-02-23T04:29:51.258000+00:00",
                "ResourceStatus": "UPDATE_IN_PROGRESS",
                "ResourceStatusReason": "User Initiated"
            }
        ]
    },
    "RestoredResources": {
        "RestoreSnapshotId": "snap-0d280e23d972abff1",
        "RestoreImageId": "ami-b45cbfc9",
        "ReplacementInstance": "i-058e15c2cb5a0efeb",
        "RestoredVolumes": [
            {
                "Attachments": [
                    {
                        "AttachTime": "2018-02-23T04:30:02+00:00",
                        "Device": "/dev/xvda",
                        "InstanceId": "i-058e15c2cb5a0efeb",
                        "State": "attached",
                        "VolumeId": "vol-0f412dda19c769e8c",
                        "DeleteOnTermination": "true"
                    }
                ],
                "AvailabilityZone": "us-east-1d",
                "CreateTime": "2018-02-23T04:30:02.645000+00:00",
                "Encrypted": "false",
                "Size": 20,
                "SnapshotId": "snap-0d280e23d972abff1",
                "State": "in-use",
                "VolumeId": "vol-0f412dda19c769e8c",
                "VolumeType": "standard"
            }
        ]
    }
    })


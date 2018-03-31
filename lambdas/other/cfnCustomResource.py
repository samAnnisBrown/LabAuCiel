import socket
import cfnresponse
from time import sleep


def lambda_handler(event, context):
    try:
        print(event)
        sleep(1)
        esEndpoint = event['ResourceProperties']['EsEndpoint']
        esIp = socket.gethostbyname(esEndpoint)

        responseData = {'esIp': esIp}
        print(responseData)
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
        responseData = {'error': e}
        cfnresponse.send(event, context, cfnresponse.FAILED, responseData)


def genpresigned():
    import boto3
    s3Client = boto3.client('s3')
    url = s3Client.generate_presigned_url('put_object', Params={'Bucket': 'ansamual-presigned-iad', 'Key': 'test2.txt'}, ExpiresIn=200)
    print(url)
    return url


url = genpresigned()
lambda_handler(
    {
        'RequestType': 'Create',
        'ServiceToken': 'arn:aws:lambda:ap-southeast-2:618252783261:function:PHD-LambdaGetElasticsearchIP-1GHVD1V3WNOBK',
        'ResponseURL': url,
        'StackId': 'arn:aws:cloudformation:ap-southeast-2:618252783261:stack/PHD/b9947d00-33df-11e8-b130-50fae957fc4a',
        'RequestId': '7b32442e-f1f4-4020-aeed-4aa39a79bcd7',
        'LogicalResourceId': 'CustomElasticsearchIP',
        'ResourceType': 'Custom::ElasticsearchIP',
        'ResourceProperties': {
            'ServiceToken': 'arn:aws:lambda:ap-southeast-2:618252783261:function:PHD-LambdaGetElasticsearchIP-1GHVD1V3WNOBK',
            'EsEndpoint': 'www.google.com'
        }
    }, None
)
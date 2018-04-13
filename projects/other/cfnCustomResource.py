import boto3
import cfnresponse

def lambda_handler(event, context):
    try:
        instance = event['ResourceProperties']['InstanceId']
        region = event['ResourceProperties']['Region']

        client = boto3.client('ec2', region_name=region)

        getvolume = client.describe_instances(
            InstanceIds=[instance],
        )

        createami = client.create_image(
            InstanceId=instance,
            Name=instance,
            NoReboot=True
        )
        responseData = {'AmiId': createami['ImageId'], 'VolumeId': getvolume['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['VolumeId']}
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except:
        cfnresponse.send(event, context, cfnresponse.FAILED)


lambda_handler('', '')


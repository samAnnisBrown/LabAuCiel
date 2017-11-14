import boto3
from core.config import get_config_item


def connect_boto_client(service, region):

    client = boto3.client(service,
                       aws_access_key_id=get_config_item('aws_access_key_id'),
                       aws_secret_access_key=get_config_item('aws_secret_access_key'),
                       region_name=region)

    return client


def connect_boto_resource(service, region):
    resource = boto3.resource(service,
                       aws_access_key_id=get_config_item('aws_access_key_id'),
                       aws_secret_access_key=get_config_item('aws_secret_access_key'),
                       region_name=region)

    return resource


class auth():

    @staticmethod
    def sts(duration=900, region=get_config_item('default_region')):
        client = connect_boto_client('sts', region)

        response = client.assume_role(
            RoleArn='arn:aws:iam::618252783261:role/labauciel-polly-s3',
            RoleSessionName='LabAuCiel_S3Access',
            DurationSeconds=duration
        )

        return response

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

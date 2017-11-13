from .connection import *


class s3():
    def put_object(bucket, key, data, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        object = client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
        )

        object.set_acl('public-read')

    def list_objects(bucket, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        output = client.list_objects_v2(
            Bucket=bucket
        )

        return output

    def get_object(bucket, key, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        output = client.get_object(
            Bucket=bucket,
            Key=key
        )

        return output





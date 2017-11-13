from .connection import *


class s3():
    def put_object(bucket, key, data):
        client = connect_boto_client('s3', get_config_item('default_region'))

        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data
        )


def create_default_config():
    print()




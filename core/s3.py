from .connection import *


class s3():

    @staticmethod
    def putObject(bucket, key, value, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=value,
        )

    @staticmethod
    def listObjects(bucket, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        output = client.list_objects_v2(
            Bucket=bucket
        )

        return output

    @staticmethod
    def getObject(bucket, key, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        output = client.getObject(
            Bucket=bucket,
            Key=key
        )

        return output

    @staticmethod
    def presignedUrl(bucket, key, method='get_object', duration=900, region=get_config_item('default_region')):
        client = connect_boto_client('s3', region)

        url = client.generate_presigned_url(
            ClientMethod=method,
            ExpiresIn=duration,
            Params={
                'Bucket': bucket,
                'Key': key,
            }
        )

        return url



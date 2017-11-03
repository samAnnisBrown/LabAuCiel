from boto3.dynamodb.conditions import Key
from decimal import Decimal
from .connection import connect_boto_resource
from .config import get_config_item
from .config import get_region_friendlyname


def create_table():
    """
    Create a new Dynamo DB table ready for LabAuCiel
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))

    try:
        table = dynamodb.Table('LabAuCiel')
        table.table_status
        """DB Already Exists.  Nothing to do."""
        return "Success", 1
    except:
        """Error I guess"""

    try:
        dynamodb.create_table(
            TableName='LabAuCiel',
            KeySchema=[
                {
                    'AttributeName': 'ID',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
    except Exception as e:
        return e, 0

    return "Success", 1


def add_item(stackname, region, instancesize, keypair, ttl, cost, starttime, endtime):
    """
    Add a new Lab entry to Dynamo DB
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    response = table.put_item(
        Item={
           "ID": stackname + starttime,
           "StackName": stackname,
           "Region": region,
           "FriendlyRegion": get_region_friendlyname(region),
           "InstanceSize": instancesize,
           "Keypair": keypair,
           "TTLMins": ttl,
           "Cost": Decimal(cost),
           "StartTime": starttime,
           "EndTime": endtime,
           "Active": 1
        })
    return response


def delete_item(stackid):
    """
    Deletes an item from Dynamo DB
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    response = table.delete_item(Key={'ID': stackid})

    return response


def update_item(key, item, value):
    """
    Update item in Dynamo DB
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    response = table.update_item(
        Key={
            'ID': key,
        },
        UpdateExpression="SET " + item + "=:updated",
        ExpressionAttributeValues={':updated': value}
    )

    return response


def get_item(name, value):
    """
    Get item from Dynamo DB
    :return:
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    response = table.get_item(Key={name: value})

    return(response)


def scan_items(key=None, value=None):
    """
    Scan all items in Dynamo DB sorted by StartTime.  Sorting is easier in Python than DDB! :P
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    if key and value:
        filtering_exp = Key(key).eq(value)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    sorted_out = sorted(response['Items'], key=lambda k: k['StartTime'], reverse=True)

    return sorted_out


def scan_items_reverse(key=None, value=None):
    """
    Scan all items in Dynamo DB sorted by StartTime in reverse order.  This could have just been a boolean on the scan_items function
    """
    dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
    table = dynamodb.Table('LabAuCiel')

    if key and value:
        filtering_exp = Key(key).eq(value)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    sorted_out = sorted(response['Items'], key=lambda k: k['StartTime'])

    return sorted_out

import json
import urllib
import dateutil
import math

from decimal import Decimal
from connection import connect_boto_client, connect_boto_resource
from ddb import add_item, scan_items, update_item, create_table
from config import update_config_item, get_config_item, get_region_friendlyname
from datetime import datetime, timedelta
from dateutil import parser

""" -------------------------------- VPCs -------------------------------- """


def list_vpcs():
    """
    TESTING: Lists all VPC IDs in all AWS Regions.
    :return: VPC IDs
    """
    for region in list_regions():
        client = connect_boto_resource('ec2', region)
        output = []
        for vpc in client.vpcs.all():
            output += [vpc.id]

    return output


""" -------------------------------- Price -------------------------------- """


def get_ec2_pricelists():
    # For all AWS Regions
    for region in list_regions():
        # Retrieve the price list
        url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/' + region + '/index.json'

        # And store it within the application
        urllib.urlretrieve(url, 'core/resources/pricelists/ec2_' + region + '.json')

    return "Updated price files successfully downloaded."


def get_ec2_price(instancesize, region, runtime_mins, multiplier):
    with open('core/resources/pricelists/ec2_' + region + '.json') as f:
        pricelist = f.read()

    data = json.loads(pricelist)
    products = data['products']
    hourlyTermCode = 'JRTCKXETXF'
    rateCode = '6YS6EN2CT7'

    for sku, properties in products.iteritems():
        if properties['productFamily'] == 'Compute Instance':
            if (properties['attributes']['instanceType'] == instancesize and
                        properties['attributes']['operatingSystem'] == 'Windows' and
                        properties['attributes']['tenancy'] == 'Shared' and
                        properties['attributes']['preInstalledSw'] == 'NA' and
                        properties['attributes']['licenseModel'] == 'No License required'):
                price = data['terms']['OnDemand'][sku][sku + '.' + hourlyTermCode]['priceDimensions'][
                    sku + '.' + hourlyTermCode + '.' + rateCode]['pricePerUnit']['USD']

                hours = int(math.ceil(float(runtime_mins) / 60))
                price = float(price) * hours
                price = round(price, 2)
                price = format(price, '.2f')

                return format(float(price) * int(multiplier), '.2f')


def get_ec2_cheapest_regions(instancesize):
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    hourlyTermCode = 'JRTCKXETXF'
    rateCode = '6YS6EN2CT7'
    region_output = []
    lowest_price = 1000.0

    for region in regions['Regions']:
        with open('core/resources/pricelists/ec2_' + region['RegionName'] + '.json') as f:
            pricelist = f.read()

        data = json.loads(pricelist)
        products = data['products']

        for sku, properties in products.iteritems():
            if properties['productFamily'] == 'Compute Instance':
                if (properties['attributes']['instanceType'] == instancesize and
                            properties['attributes']['operatingSystem'] == 'Windows' and
                            properties['attributes']['tenancy'] == 'Shared' and
                            properties['attributes']['preInstalledSw'] == 'NA' and
                            properties['attributes']['licenseModel'] == 'No License required'):
                    price = data['terms']['OnDemand'][sku][sku + '.' + hourlyTermCode]['priceDimensions'][
                        sku + '.' + hourlyTermCode + '.' + rateCode]['pricePerUnit']['USD']

                    if float(price) == lowest_price:
                        region_output.append(get_region_friendlyname(region['RegionName']))

                    if float(price) < lowest_price:
                        del region_output[:]
                        region_output.append(get_region_friendlyname(region['RegionName']))
                        lowest_price = float(price)

    return region_output


""" -------------------------------- CloudFormation -------------------------------- """


def list_global_cf_stacks():
    """
    TESTING: Lists all CloudFormation stacks in all regions.
    """
    for region in list_regions():
        client = connect_boto_client('cloudformation', region)
        response = client.describe_stacks()
        print response['Stacks']


def get_cf_stack_status(stackname, region):
    client = connect_boto_client('cloudformation', region)

    try:
        response = client.describe_stacks(
            StackName=stackname
        )
        response = response['Stacks'][0]['StackStatus']
    except:
        response = "NOT_EXIST"

    return response


def create_cf_stack(stackname, region, instance, keypair, userpassword, ttl, cost, labno):
    client = connect_boto_client('cloudformation', region)

    # Read CloudFormation Template
    with open('core/resources/cf_template.json') as f:
        template = f.read()

    # Update S3 default details
    template = template.replace('s3_bucket', get_config_item('s3_bucket_name'))
    template = template.replace('s3_region', get_config_item('default_region'))

    # Calculate Times
    starttime = datetime.utcnow().isoformat()
    endtime = (datetime.utcnow() + timedelta(minutes=int(ttl))).isoformat()

    # Get region's AMI ID for Windows 2016
    amiid = get_ami_id(region)

    # Submit CloudFormation Template
    cfstackname = stackname

    for i in range(int(labno)):
        if int(labno) > 1:
            cfstackname = stackname + str(i + 1)
        try:
            client.create_stack(
                StackName=cfstackname,
                TemplateBody=template,
                Parameters=[
                    {
                        'ParameterKey': 'InstanceSize',
                        'ParameterValue': instance,
                        'UsePreviousValue': False
                    },
                    {
                        'ParameterKey': 'KeyPair',
                        'ParameterValue': keypair,
                        'UsePreviousValue': False
                    },
                    {
                        'ParameterKey': 'TagName',
                        'ParameterValue': cfstackname,
                        'UsePreviousValue': False
                    },
                    {
                        'ParameterKey': 'AmiId',
                        'ParameterValue': amiid,
                        'UsePreviousValue': False
                    },
                    {
                        'ParameterKey': 'UserPassword',
                        'ParameterValue': userpassword,
                        'UsePreviousValue': False
                    },
                    {
                        'ParameterKey': 'TTL',
                        'ParameterValue': endtime,
                        'UsePreviousValue': False
                    }
                ],
                Capabilities=[
                    'CAPABILITY_IAM'
                ],
            )
        except Exception, e:
            return str(e)

        # Update Database
        add_item(cfstackname, region, instance, keypair, ttl, cost, starttime, endtime)

    return "Success"


def delete_cf_stack(stackname, region, stackid, starttime, instancesize):
    cloudformation = connect_boto_resource('cloudformation', region)
    stack = cloudformation.Stack(stackname)

    # Get time information
    stime = dateutil.parser.parse(starttime)
    etime = dateutil.parser.parse(datetime.utcnow().isoformat())

    # Calculate Runtime and get Cost
    actual_runtime_mins = (etime - stime).total_seconds() / 60
    newcost = get_ec2_price(instancesize, region, float(actual_runtime_mins), 1)

    # Update DB with actual cost
    update_item(stackid, 'Cost', Decimal(newcost))

    # Delete the CloudFormation Stack
    response = stack.delete()
    update_item(stackid, 'Active', 0)

    return newcost


""" -------------------------------- Regions -------------------------------- """


def list_regions():
    """
    Lists all AWS Regions
    ":return: A list of regions
    """
    client = connect_boto_client('ec2', 'ap-southeast-2')

    response = client.describe_regions()
    output = []
    for i in range(response['Regions'].__len__()):
        output += [response['Regions'][i]['RegionName']]

    return output


def get_region_json():
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    regions_string = str(regions['Regions'])[1:-1]

    return json.dumps(regions_string)


""" -------------------------------- S3 -------------------------------- """


def create_s3_documents(bucket=get_config_item('s3_bucket_name')):
    region = get_config_item('default_region')
    client = connect_boto_client('s3', region)

    try:
        with open('core/resources/LabAuCielDeleteStack.ps1') as stackfile:
            client.put_object(
                Bucket=bucket,
                Key="LabAuCielDeleteStack.ps1",
                Body=stackfile.read()
            )
        with open('core/resources/LabAuCielPostBoot.ps1') as bootfile:
            client.put_object(
                Bucket=bucket,
                Key="LabAuCielPostBoot.ps1",
                Body=bootfile.read()
            )
    except Exception, e:
        return str(e), 0

    return str("Success"), 1


def create_s3_bucket(bucketname):
    region = get_config_item('default_region')
    client = connect_boto_client('s3', region)

    if region != 'us-east-1':
        try:
            client.create_bucket(
                Bucket=bucketname,
                CreateBucketConfiguration={
                    'LocationConstraint': region
                },
            )
            return "Success", 1
        except Exception, e:
            return str(e), 0
    else:
        try:
            client.create_bucket(
                Bucket=bucketname
            )
            return "Success", 1
        except Exception, e:
            return str(e), 0


def get_s3_documents():
    client = connect_boto_client('s3', get_config_item('default_region'))

    response = client.list_objects_v2(
        Bucket='labauciel'
    )
    cf_stack = False
    postboot = False

    for file in response['Contents']:
        if file['Key'] == 'LabAuCielDeleteStack.ps1':
            cf_stack = True
        if file['Key'] == 'LabAuCielPostBoot.ps1':
            postboot = True

    if (postboot == True) and (cf_stack == True):
        response = 1
    else:
        response = 0

    return response


""" -------------------------------- EC2 -------------------------------- """


def list_keypairs(region):
    """
    Lists all keypairs in the current region
    :param region: AWS Region Name
    :return: List of keypairs
    """
    client = connect_boto_client('ec2', region)
    response = client.describe_key_pairs()

    return response['KeyPairs']


def create_key_pair(region):
    client = connect_boto_client('ec2', region)

    # Create a LabAuCiel key
    response = client.create_key_pair(KeyName='LabAuCiel_Key')

    # Return the key details for download
    return response['KeyMaterial']


def get_ami_id(region):
    client = connect_boto_client('ec2', region)

    filters = [{'Name': 'name', 'Values': ['*2016*English*Full*Base*']}]
    response = client.describe_images(
        Filters=filters,
        Owners=['amazon']
    )
    output = []

    for i in range(response['Images'].__len__()):
        output += [response['Images'][i]]

    latest = None

    for image in output:
        if not latest:
            latest = image
            continue

        if parser.parse(image['CreationDate']) > parser.parse(latest['CreationDate']):
            latest = image

    return latest['ImageId']


def get_ec2instance(stackname, region):
    client = connect_boto_client('cloudformation', region)

    response = client.describe_stack_resource(
        StackName=stackname,
        LogicalResourceId='LabInstance'
    )

    return response


def get_ec2instance_endtime(stackname, region):
    stack = get_ec2instance(stackname, region)
    instanceid = stack['StackResourceDetail']['PhysicalResourceId']

    ec2 = connect_boto_resource('ec2', region)
    instance = ec2.Instance(instanceid)
    for tags in instance.tags:
        if tags["Key"] == stackname + 'TTL':
            endtime = tags["Value"]

    return endtime


def get_ec2instance_ip(stackname, region):
    stack = get_ec2instance(stackname, region)
    instanceid = stack['StackResourceDetail']['PhysicalResourceId']

    ec2 = connect_boto_resource('ec2', region)
    instance = ec2.Instance(instanceid)
    instanceip = instance.public_ip_address

    return instanceip


""" -------------------------------- Lab Status and Updates -------------------------------- """


def active_labs():
    activelabs = scan_items('Active', 1)

    if activelabs.__len__() < 1:
        return 0
    else:
        return 1


def update_global_lab_status():
    activelabs = scan_items('Active', 1)

    for lab in activelabs:
        status = get_cf_stack_status(lab['StackName'], lab['Region'])
        if status == 'NOT_EXIST':
            update_item(lab['ID'], 'Active', Decimal(0))

    return "Done"


def update_running_lab_ips():
    # Get a list of running labs
    activelabs = scan_items('Active', 1)

    # If it doesn't have a public IP attached, check to see if it has one and update
    for labs in activelabs:
        try:
            ipexists = labs['PublicIP']
            if ipexists is None:
                raise
        except:
            update_item(labs['ID'], 'PublicIP', get_ec2instance_ip(labs['StackName'], labs['Region']))

    return "Done"


def update_instance_endtime(stackname, region, stackid, add_mins, instancesize, currentcost):
    # Calculate Dates
    currentendtime = dateutil.parser.parse(get_ec2instance_endtime(stackname, region))
    newendtime = currentendtime + timedelta(minutes=int(add_mins))

    # Updated EC2 Tag with updated time
    ec2instance = get_ec2instance(stackname, region)
    ec2 = connect_boto_client('ec2', region)
    try:
        ec2.create_tags(
            Resources=[
                ec2instance['StackResourceDetail']['PhysicalResourceId'],
            ],
            Tags=[
                {
                    'Key': stackname + 'TTL',
                    'Value': str(newendtime)
                }
            ]
        )
    except Exception, e:
        print str(e)
        return str(e), 0

    # Calculate additional cost
    extracost = get_ec2_price(instancesize, region, add_mins, 1)
    newcost = float(currentcost) + float(extracost)

    # Update DB
    update_item(stackid, 'EndTime', str(newendtime))
    update_item(stackid, 'Cost', Decimal(str(newcost)))

    return newendtime


def update_credentials(key, secretkey):
    update_config_item('aws_access_key_id', str(key))
    update_config_item('aws_secret_access_key', str(secretkey))
    return str("Done"), 1


""" -------------------------------- Testers -------------------------------- """


def test_aws_connection():
    ec2 = connect_boto_client('ec2', get_config_item('default_region'))

    try:
        ec2.describe_regions()
        response = "AWS Connection Established Successfully"
        errorlevel = 1
    except:
        response = "Connection to AWS Failed.  Please check your credentials"
        errorlevel = 0

    return response, errorlevel


def test_db_connection():
    try:
        dynamodb = connect_boto_resource('dynamodb', get_config_item('default_region'))
        table = dynamodb.Table('LabAuCiel')
        table.table_status
        response = "AWS Connection Established Successfully"
        errorlevel = 1
    except:
        response = "Connection to AWS Failed.  Please check your credentials"
        errorlevel = 0

    return response, errorlevel


""" -------------------------------- Initialisation -------------------------------- """


def initial_config(s3_bucket):
    ct = create_table()
    if ct[1] == 0:
        return str(ct[0]), 0

    cb = create_s3_bucket(s3_bucket)
    if cb[1] == 0:
        return str(cb[0]), 0

    cd = create_s3_documents(s3_bucket)
    if cd[1] == 0:
        return str(cd[0]), 0

    update_config_item('initialised', 1)
    update_config_item('s3_bucket_name', str(s3_bucket))
    return "Success", 1

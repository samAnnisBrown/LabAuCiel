import boto3

ec2 = boto3.resource('ec2')
vpc = ec2.Vpc('vpc-445fb623')
response = vpc.cidr_block
print(response)
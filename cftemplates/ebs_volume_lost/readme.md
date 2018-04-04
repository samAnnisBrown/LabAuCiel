## AWS Health AWS_EBS_VOLUME_LOST

### Description
Underlying hardware related to your EBS volume has failed, and the data associated with the volume is unrecoverable.
If you have an EBS snapshot of the volume, you need to restore that volume from your snapshot. 
This tools checks if the failed volume has a snapshot and is part of a root volume on an EC2 instance.
Tool will restore the instance root volume from latest snapshot automatically if it does.
Notification on update will be sent to SNS topic assigned.

[![Launch EBS VOLUME LOST Stack into N. Virginia with CloudFormation](http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/images/cloudformation-launch-stack-button.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=AWSEBSVolLost&templateURL=https://s3.amazonaws.com/aws-health-tools-assets/cloudformation-templates/aws_ebs_vol_lost_cfn.yaml)

[![Launch IMPORTANT APP Stack into N. Virginia with CloudFormation](http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/images/cloudformation-launch-stack-button.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=AWSEBSVolLost&templateURL=https://s3.amazonaws.com/aws-health-tools-assets/cloudformation-templates/aws_ebs_vol_lost_importantapp-cfn.yaml)

### Setup
1.  Launch the first CloudFormation template (**aws_ebs_vol_lost_cfn.yaml**).
    * This template will build out the required Step and Lambda functions that will action a Personal Health Dashboard event.  It also creates a small Elasticsearch domain for visualisation.
1.  Launch the second CloudFormation template (**aws_ebs_vol_lost_importantapp-cfn.yaml**).
    * This template will build out a mock application that will be impacted by an EBS service disruption

##### Creating a Mock Event

1.  With both CloudFormation stacks completed - copy the **VolumeId** from the Outputs of the **aws_ebs_vol_lost_importantapp-cfn.yaml** stack.
1.  Replace the **vol-xxxxxxxxxxxxxxxxx** values in the **phd-mock-event.json** with the copied value.
1.  Increment the value of **id** and change the **time** to within the past 14 days in **phd-mock-event.json**.
1.  Navigate to StepFunctions, and enter the State Machine **VolumeLost-**
1.  Create a **New Execution**, pasting the contents of your modified **phd-mock-event.json** (i.e. with the **VolumeId**, **id**, and **time** updated).  This will trigger a replacement of the ImportantApp EC2 Instance.  Wait for this to complete.
1.  Open the Kibana dashboard (the URL can be found in the Outputs of the **aws_ebs_vol_lost_cfn.yaml** CloudFormation Stack)
1. In Kibana, under **Management > Index Patterns**, create an index pattern named **phd-events** using **PhdEventTime** as the **Time Filter**.
1. Under **Management > Saved Objects**, import **elasticsearch-objects.json**, overwriting all objects, and using **phd-events** as the new index pattern.
1. Navigate to **Dashboard > PHD Events** to see the event(s).
1. Repeat steps 1 to 5 to create additional mock events, modifying **id** and **time**.

#### CloudFormation
Choose **Launch Stack** to launch the CloudFormation template in the US East (N. Virginia) Region in your account:

The CloudFormation template requires the following parameters:

*SNS topic* - Enter the SNS topic to send notification to.

#### Warnings

These CloudFormation templates are for demo and proof-of-concept purposes only.  They and are not intended for production environments.  Amongst other deficiencies, they:
* do not follow the rule of least privileged access, and will create IAM Roles with the 'AdministratorAccess' AWS Managed policy
* will serve public traffic from the Elasticsearch domain over unencrypted HTTP connections

### License
AWS Health Tools are licensed under the Apache 2.0 License.

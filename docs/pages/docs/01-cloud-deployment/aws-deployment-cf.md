---
title: AWS CloudFormation rake Deployment
keywords: cloud, deployment, aws, cloudformation
tags: [getting-started, deployment, cloud, aws, cloudformation]
summary: "Deploying Grakn on AWS using Cloudformation"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/aws-deployment-cf
folder: docs
---

# CloudFormation
After initiating launch, we arrive at the CloudFormation stack creation page:

![](/images/aws-cloudformation.png).

To proceed, simply press the `Next` button and you will be taken to the stack parameter page;

![](/images/aws-cloudformation-config.png).

## Stack parameters
The following parameters and parameter groups are used to define the stack:

* General:
    - **StackName**
    
        The name of your Grakn KGMS stack. This must be a valid system name, specifically, it must consist of only lowercase letters, numbers, and hyphens, and cannot exceed 50 characters. 


* Node Configuration:
    - **GraknGroupSize**
    
        Number of Grakn instances in the cluster.
    - **GraknInstanceType**
    
        EC2 instance type of Grakn instances.
    
    - **EbsVolumeSize**
    
        Size in GB of each of the EBS volumes that are attached to Grakn instances.
    - **OptimiseForEbs**
    
        Specifies whether to optimise Grakn Launch Configuration for EBS I/O.     
    
* VPC/Network:
    - **VPC**
    
        The id of the VPC the stack should be deployed into.

    - **GraknSubnetCidrBlock**
   
        The Grakn cluster will be deployed in a separate subnet and this setting specifies its IP CIDR range.
    

* Security Group:

    - **SSHLocation**
     
        The IP address range that can be used to access Grakn instances using SSH.
    - **gRPCLocation**
   
        The IP address range that can be used to make gRPC requests.

* Key Pair:
     - **KeyPairName**
                     
        The key pair assigned to Grakn instances to allow SSH access.
        
Once satisfied, press `Next` to proceed and arrive at the Options screen:

![](/images/aws-cloudformation-options.png).

where you can adjust Tagging, Permissions and other options to your liking. Once done, press `Next` to arrive at the final Review screen:

![](/images/aws-cloudformation-review.png).

If happy with the deployment, press `Create` to start the deployment of the Grakn stack.
        
## **IMPORTANT:** Running Grakn
**A Grakn Cluster starts automatically running as user `grakn`.** There is no need to manually start grakn servers.
**Once the deployment is started, please allow some time for the cluster to fully bootup and synchronise**. A reasonable rule of thumb for the bootup time is **2 minutes per cluster node**. The progress of cluster bootup can be
checked by logging in to a cluster node and executing the [cluster health check](#cluster-check) command.

## **IMPORTANT:** Scaling the cluster
Grakn cluster is deployed within an Auto Scaling Group which allows you to adjust the number of instances in the cluster in a straight-forward manner.
Auto Scaling Groups group together EC2 instances that share similar characteristics and are treated as a logical grouping for the purposes of instance scaling and management.

To scale your cluster please go to the Auto Scaling and then to the Auto Scaling Groups section of your EC2 service dashboard which should look along the lines of:

![](/images/aws-autoscaling.png).

There you can adjust the instance count to your needs by changing the `Desired Capacity` parameter.

For more information on Auto Scaling Groups please visit [AWS Docs](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html).

## **IMPORTANT:** Stopping/starting Grakn instances within the cluster

By design, it is not possible to stop an instance belonging to an Auto Scaling Group. When a Scaling Policy triggers the removal of an instance, Auto Scaling will always Terminate the instance. As a result a different
procedure is needed for stopping instances and it will be described here.

**IMPORTANT:** Before you proceed, make sure that the minimum capacity of the Auto Scaling Group the instance belongs to is at least one smaller than the current instance count, e.g. if you have 3 instances running and want to stop one, 
make sure the minimum capacity is at most 2. Otherwise new instances will be created in place of the stopped instance.

The procedure is the following, for an instance you want to stop:
- detach it from the Auto Scaling Group. In AWS CLI this can be achieved by typing:
```
aws autoscaling detach-instances --instance-ids <instance-id> --auto-scaling-group-name <asg> --should-decrement-desired-capacity
```
- stop the detached instance. 
```
aws ec2 stop-instances --instance-ids <instance-id>
```

When you find it fit to restart the instance, the procedure follows in the reverse manner:

- start your instance
```
aws ec2 start-instances --instance-ids <instance-id>
```
- attach the instance back to the Auto Scaling Group
```
aws autoscaling attach-instances --instance-ids <instance-id> --auto-scaling-group-name <asg>
```

More information on stopping/starting and attaching and detaching instances can be found here:
- [Stopping and Starting instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Stop_Start.html),
- [Attaching instances](https://docs.aws.amazon.com/autoscaling/ec2/userguide/attach-instance-asg.html),
- [Detaching instances](https://docs.aws.amazon.com/autoscaling/ec2/userguide/detach-instance-asg.html).
        
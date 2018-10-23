---
title: AWS Deployment
keywords: cloud, deployment, aws
tags: [getting-started, deployment, cloud, aws]
summary: "Deploying Grakn on AWS"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/aws-deployment
folder: docs
---

# AWS Marketplace

## Deployment

We shall begin with the deployment of Grakn instances. The procedure is straight-forward and takes advantage of the listing in the [AWS Marketplace](https://aws.amazon.com/marketplace/pp/B07H8RMX5X).

![](/images/aws-solution-listing.png)

At the moment we offer two deployment options:
- **CloudFormation**

AWS CloudFormation templates are JSON or YAML formatted files that simplify resource orchestration, provisioning and management on AWS. The template describes the service or application
architecture and configuration and AWS CloudFormation uses the template to provision the required resources (such as EC2 instances, EBS storages, etc.). The deployed application together with its 
associated resources is referred to as a `stack`.
- **Grakn KGMS Amazon Machine Image (AMI)**

Provides a Grakn-equipped image that can be used when launching instances. The AMI is specified when starting an instance and you are free to launch as many instances as need be, 
combine them with instances using different AMIs and orchestrate them.

To commence deployment, click yellow `Continue to Subscribe` button. Once subscribed, you should see the method configuration choice screen:

![](/images/aws-deployment-methods.png)

which allows you to either pick the [AMI](#grakn-ami):
 
![](/images/aws-deployment-ami.png)
 
or the [CloudFormation](#grakn-cloudformation) options.

![](/images/aws-deployment-cloudformation.png).

To proceed please pick the required option and press the `Continue to Launch` button.
 
## <a name="grakn-ami"></a> Grakn AMI 
The AMI launch screen looks as follows:
![](/images/aws-deployment-ami-launch.png).

and allows you to specify the instance parameters. After having specified the parameters, press the launch button to start a Grakn instance.  

The AMI Provides an image with a preinstalled Grakn KGMS. The following list summarises the important locations:

- Grakn dist: `/opt/grakn/`
- Grakn config: /`opt/grakn/conf/grakn.properties`
- logs: `/var/log/grakn/`


#### Important: storage configuration
By default Grakn is expecting the storage directory to be `/mnt/data1/`. We recommend attaching an EBS drive to the Grakn instance and mounting it so that storage is located on the EBS drive.
The instructions how to attach the EBS drive can be found [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-attaching-volume.html).

In order to mount the drive, please follow the following procedure:
* identify the EBS block name and path, to do this run `lsblk` command and find the name of attached EBS device. The path will then be `/dev/<BLOCK_NAME>`.
* if the drive is not formatted, format it by executing:
```
mkfs -t ext4 <BLOCK_HOME>
```

* mount the drive by executing:
```
mount <BLOCK_PATH> /mnt/data1/
```

#### Running
Grakn is configured as a service and by default it is not running. Once you have configured the storage, to start grakn the following command needs to be executed as root on the target machine:
```
systemctl grakn start
```

To stop run:

```
systemctl grakn stop
```


## <a name="grakn-cloudformation"></a> CloudFormation
After initiating launch, we arrive at the CloudFormation stack creation page:

![](/images/aws-cloudformation.png).

To proceed, simply press the `Next` button and you will be taken to the stack parameter page;

![](/images/aws-cloudformation-config.png).

### Stack parameters
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
        
### Running Grakn
**A Grakn Cluster starts automatically running as user `grakn`.** There is no need to manually start grakn servers.
**Once the deployment is started, please allow some time for the cluster to fully bootup and synchronise**. A reasonable rule of thumb for the bootup time is **2 minutes per cluster node**. The progress of cluster bootup can be
checked by logging in to a cluster node and executing the [cluster health check](#cluster-check) command.

### Scaling the cluster
Grakn cluster is deployed within an Auto Scaling Group which allows you to adjust the number of instances in the cluster in a straight-forward manner.
Auto Scaling Groups group together EC2 instances that share similar characteristics and are treated as a logical grouping for the purposes of instance scaling and management.

To scale your cluster please go to the Auto Scaling and then to the Auto Scaling Groups section of your EC2 service dashboard which should look along the lines of:

![](/images/aws-autoscaling.png).

There you can adjust the instance count to your needs by changing the `Desired Capacity` parameter.

For more information on Auto Scaling Groups please visit [AWS Docs](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html).

### **IMPORTANT:** Stopping/starting Grakn instances within the cluster

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
        
## User credentials
In order to use Graql and Grakn consoles, user credentials are required. The default user is `grakn`, whereas the default password can be found in the `GraknUserPassword` output.

**Once logged in, We strongly encourage you to change the default user password**. In order to do so, log in to the Grakn console and type:
 
```
UPDATE USER grakn WITH PASSWORD newpassword
```

More details on available commands can be found [here](http://dev.grakn.ai/docs/get-started/grakn-console). 
        
## Accessing Grakn

There are various ways to access Grakn on AWS. Here we will address the most common usage patterns.

### Using Grakn gRPC client

The most common access pattern is to use the Grakn gRPC client to connect to a Grakn cluster. The connection happens via TCP port 48555 and the `gRPCLocation` stack parameter defines
the range of IP addresses that can schedule gRPC requests.
        
### Logging in into a node
To log in into one of the cluster nodes, simply use the ssh command:

`ssh -i <private key file> ubuntu@<grakn instance DNS name or IP address>`

#### <a name="cluster-check"></a> Cluster health check
To verify the state of the cluster, execute the following command:
    
`grakn cluster status`
     
A sample output of the command executed on a node of a healthy 3-node cluster shall look similar to this:

![](/images/aws-cluster-health.png)

#### Accessing the Graql console

To access the Graql console, a user password is required. The default user password is specified in the `GraknUserPassword` output.

To log into the Graql console, simply type `graql console`. After entering the user credentials (user: grakn, password: the one specified in the `GraknUserPassword` output), you are free to interact with Grakn via the Graql terminal. 
A successful login attempt shall look like this:

![](/images/aws-graql-console.png)

A summary of available commands can be found [here](http://dev.grakn.ai/docs/get-started/graql-console).

#### Accessing the Grakn console
The Grakn console can be accessed similarly to the Graql console by typing:

`grakn console start`
  
and providing the user credentials. A successful login attempt will look like this:

![](/images/aws-grakn-console.png)

A summary of available commands can be found [here](http://dev.grakn.ai/docs/get-started/grakn-console).

## Next Steps

If you want to learn more about Grakn KGMS, the [Grakn Academy](https://dev.grakn.ai/academy/) is a good place to start.

To learn more about running Grakn KGMS in the cloud, take a look at the [best practices guide](https://dev.grakn.ai/docs/cloud-deployment/best-practices)
and [post deployment steps](https://dev.grakn.ai/docs/cloud-deployment/post-deployment).

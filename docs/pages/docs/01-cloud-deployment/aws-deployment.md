---
title: AWS Deployment
keywords: cloud, deployment, aws
tags: [getting-started, deployment, cloud]
summary: "Deploying Grakn on AWS"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/aws-deployment
folder: docs
---

# AWS Marketplace

## Deployment

TBC

### Stack parameters
The following parameters allow to define the stack details:

* General:
    - **StackName**
    
        Name for you Grakn KGMS stack. This must be a valid system name, specifically, it must consist of only lowercase letters, numbers, and hyphens and cannot exceed 50 characters. 
      
    - **KeyPairName**
        
        The key pair assigned to Grakn instances to allow SSH access.
    
* Network:
    - **VPC**
    
        The id of the VPC the stack should be deployed into.
    - **InternetFacingRouteTable**
      
        The id of a internet-facing Route Table in the VPC.
    - **GraknSubnetCidrBlock**
   
        The Grakn cluster will be deployed in a separate subnet and this setting specifies its IP CIDR range.
    - **SSHLocation**
     
        The IP address range that can be used to access Grakn instances using SSH.
    - **gRPCLocation**
   
        The IP address range that can be used to make gRPC requests.
     

* Cluster:
    - **GraknInstanceType**
    
        EC2 instance type of a single Grakn instance.
    - **GraknGroupSize**
    
        Number of Grakn instances in the cluster.
    - **EbsVolumeSize**
    
        Size in GB of each of EBS volumes that are attached to Grakn instances.
    - **OptimiseForEbs**
    
        Specifies whether the Grakn launch configuration should optimised for EBS I/O.
        
## Accessing Grakn

There are various ways to access Grakn on AWS. Here we will address the most common usage patterns.

### Using gRPC

The most common access pattern is to use a gRPC client to connect to a Grakn cluster. The connection happens via TCP port 48555 and the `gRPCLocation` stack parameter defines
the range of IP addresses that can schedule gRPC requests.
        
### Logging into a node
To log in into one of the cluster nodes, simply use the ssh command:

`ssh -i <private key file> ubuntu@<grakn instance DNS name or IP address>`

#### Cluster health check
To verify cluster state, execute the following command:
    
`grakn cluster status`
     
A sample output of the command executed on a node of a healthy 3-node cluster shall look similar to this:

![](/images/aws-cluster-health.png)

#### Accessing the Graql console

To access the Graql console, a user password is required. The default user password is specified in the `GraknUserPassword` output.

To log into the Graql console, simply type `graql console`. After entering the user credentials (user: grakn, password: the one from Google console), you are free to interact with Grakn via the Graql terminal. 
A succesful login attempt shall look like this:

![](/images/aws-graql-console.png)

A summary of available commands can be found [here](http://dev.grakn.ai/docs/get-started/graql-console).

#### Accessing the Grakn console
The Grakn console can be accessed similarly to graql console by typing:

`grakn console start`
  
and providing the user credentials (user: grakn, password: the one from Google console). A successful login attempt will look like this:

![](/images/aws-grakn-console.png)

Provided you log in as user with `admin` privileges, Grakn console allows to perform the following actions:

* create a new user:

`CREATE USER username WITH PASSWORD userpassword WITH ROLE admin`

* update existing user's password

`UPDATE username WITH PASSWORD newpassword`

* retrieve all the users present:

`LIST USERS`

* retrieve a user:

`GET USER username`

* delete an existing user:

`DELETE USER username`

## Next Steps

If you want to learn more about Grakn KGMS, the [Grakn Academy](https://dev.grakn.ai/academy/) is a good place to start.

To learn more about running Grakn KGMS in the cloud, take a look at the [best practices guide](https://dev.grakn.ai/docs/cloud-deployment/best-practices)
and [post deployment steps](https://dev.grakn.ai/docs/cloud-deployment/post-deployment).

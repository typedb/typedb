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
The following parameters are used to define the stack:

* General:
    - **StackName**
    
        The name of your Grakn KGMS stack. This must be a valid system name, specifically, it must consist of only lowercase letters, numbers, and hyphens, and cannot exceed 50 characters. 
      
    - **KeyPairName**
        
        The key pair assigned to Grakn instances to allow SSH access.
    
* Network:
    - **VPC**
    
        The id of the VPC the stack should be deployed into.
    - **InternetFacingRouteTable**
      
        The id of an internet-facing Route Table in the VPC.
    - **GraknSubnetCidrBlock**
   
        The Grakn cluster will be deployed in a separate subnet and this setting specifies its IP CIDR range.
    - **SSHLocation**
     
        The IP address range that can be used to access Grakn instances using SSH.
    - **gRPCLocation**
   
        The IP address range that can be used to make gRPC requests.
     

* Cluster:
    - **GraknInstanceType**
    
        EC2 instance type of Grakn instances.
    - **GraknGroupSize**
    
        Number of Grakn instances in the cluster.
    - **EbsVolumeSize**
    
        Size in GB of each of the EBS volumes that are attached to Grakn instances.
    - **OptimiseForEbs**
    
        Specifies whether to optimise Grakn Launch Configuration for EBS I/O.
        
## Running Grakn
**A Grakn Cluster starts automatically running as user `grakn`.** There is no need to manually start grakn servers.
**Once the deployment is started, please allow some time for the cluster to fully bootup and synchronise**. A reasonable rule of thumb for the bootup time is **2 minutes per cluster node**. The progress of cluster bootup can be
checked by logging in to a cluster node and executing the [cluster health check](#cluster-check) command.

        
## User credentials
In order to use Graql and Grakn consoles, user credentials are required. The default user is `grakn`, whereas the default password can be found in the `GraknUserPassword` output.

**Once logged in, We strongly encourage to change the default user password**. In order to do so, log in to th Grakn console and type:
 
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

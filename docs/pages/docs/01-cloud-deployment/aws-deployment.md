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
- [**CloudFormation**](http://dev.grakn.ai/docs/cloud-deployment/aws-deployment-cloudformation)

AWS CloudFormation templates are JSON or YAML formatted files that simplify resource orchestration, provisioning and management on AWS. The template describes the service or application
architecture and configuration and AWS CloudFormation uses the template to provision the required resources (such as EC2 instances, EBS storages, etc.). The deployed application together with its 
associated resources is referred to as a `stack`.
- [**Grakn KGMS Amazon Machine Image (AMI)**](http://dev.grakn.ai/docs/cloud-deployment/aws-deployment-ami)

Provides a Grakn-equipped image that can be used when launching instances. The AMI is specified when starting an instance and you are free to launch as many instances as need be, 
combine them with instances using different AMIs and orchestrate them.

To commence deployment, click yellow `Continue to Subscribe` button. Once subscribed, you should see the method configuration choice screen:

![](/images/aws-deployment-methods.png)

which allows you to either pick the [AMI](http://dev.grakn.ai/docs/cloud-deployment/aws-deployment-ami):
 
![](/images/aws-deployment-ami.png)
 
or the [CloudFormation](http://dev.grakn.ai/docs/cloud-deployment/aws-deployment-cloudformation) options.

![](/images/aws-deployment-cloudformation.png).

To proceed please pick the required option, press the `Continue to Launch` button and refer to specific parts of the documentation
addressing your deployment option of choice.
 
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

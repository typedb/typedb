---
title: AWS AMI Deployment
keywords: cloud, deployment, aws, ami
tags: [getting-started, deployment, cloud, aws, ami]
summary: "Deploying Grakn on AWS using AMI"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/aws-deployment-ami
folder: docs
---

# AWS Grakn AMI Deployment
The AMI launch screen looks as follows:
![](/images/aws-deployment-ami-launch.png).

and allows you to specify the instance parameters. After having specified the parameters, press the launch button to start a Grakn instance.  

The AMI Provides an image with a preinstalled Grakn KGMS. The following list summarises the important locations:

- Grakn dist: `/opt/grakn/`
- Grakn config: /`opt/grakn/conf/grakn.properties`
- logs: `/var/log/grakn/`


## **IMPORTANT:** Storage configuration
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

## **IMPORTANT:** Running
Grakn is configured as a service and by default it is not running. Once you have configured the storage, to start grakn the following command needs to be executed as root on the target machine:
```
systemctl grakn start
```

To stop run:

```
systemctl grakn stop
```
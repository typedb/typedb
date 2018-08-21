---
title: Deployment Best Practices
keywords: cloud, deployment, google
tags: [getting-started, deployment, cloud]
summary: "Best Practices for deploying Grakn on Google Cloud Platform"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/best-practices
folder: docs
---

# Deployment Best Practices

In this section we shall describe the recommendations for compute and storage aspects of cloud deployments.

## Compute

The optimum machine choice offering a good balance between CPU and memory should be equipped with at least 4 vCPUs and 8 GB of RAM.
Using machines with additional RAM above a 25 GB threshold is not expected to yield significant performance improvements.
Having these bounds in mind the following machines are recommended because they offer a balanced set of system resources for a range of workloads:

On Google cloud:

* Standard: 
    - n1-standard-4, 
    - n1-standard-8, 
    - n1-standard-16
* High-CPU: 
    - n1-highcpu-16,
    - n1-highcpu-32
* High-memory: 
    - n1-highmem-4,
    - n1-highmem-8

On AWS:

* General Purpose: 
    - t2.xlarge, 
    - t2.2xlarge,
* Memory Optimised: 
    - m5.xlarge, 
    - m4.xlarge,
    - m3.xlarge, 
* Compute Optimised: 
    - c5.xlarge,
    - c5.2xlarge,
    - c4.xlarge,
    - c4.2xlarge, 
    - c3.xlarge,
    - c3.2xlarge,


The optimal machine type appropriate for a given use case shall depend on the specific performance requirements of the use case.

For more information on machine types, please visit: 
* [GC Machine Types](https://cloud.google.com/compute/docs/machine-types)
* [AWS EC2 Instance Types](https://aws.amazon.com/ec2/instance-types)

## Storage

Google Cloud offers a wide spectrum of storage options. For performance, we suggest using SSD persistent disks for majority of use cases. The specific size of persistent disks depends on the volume of data to be processed and can be tailored to needs.

It is also possible to use HDD persistent disks. Although these come at a reduced price, their poor performance does not justify their use and we do not recommend them.

For more information on GCE disks, please visit the [GC Disk Docs](https://cloud.google.com/compute/docs/disks)

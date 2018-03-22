# GC Deployment Best Pracices

In this section we shall describe the recommendations for comput and storage aspects of GC deployments

## Compute

The optimum machine choice offering a good balance between CPU and memory should be equipped with at least 4 vCPUs and 8 GB of RAM.
Using machines with extra RAM amount above a 25 GB threshold is not expected to yield significant performance improvements.
Having these bounds in mind the following machines are recommended because they offer a balancd system resources for a range of workloads:

* n1-standard-4
* n1-standard-8
* n1-standard-16
* n1-highcpu-16
* n1-highcpu-32
* n1-highmem-4
* n1-highmem-8

The optimal machine type appropriate for a given use case shall depend on the specific performance requirements of the use case.

For more information on machine types, please visit: https://cloud.google.com/compute/docs/machine-types

## Storage

Google Cloud offers a wide spectrum of storage options. For performance, we suggest using SSD persistent disks for majority of use cases. The specific size of persistent disks depends on the volume of data to be processed and can be tailored to needs.

It is also possible to use HDD persistent disks. Although these come at a reduced price, their poor performance does not justify their use and we do not recommend them.

For more information on GCE disks, please visit: https://cloud.google.com/compute/docs/disks

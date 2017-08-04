---
title: Grakn Deployment Guide
keywords: setup, getting started, download
last_updated: December 15th 2016
tags: [getting-started]
summary: "A guide to setting up a production deployment of GRAKN.AI."
sidebar: documentation_sidebar
permalink: /documentation/deploy-grakn/grakn-deployment-guide.html
folder: documentation
comment_issue_id: 24
---


## Introduction

This guide offers advice on how to:

* run Grakn in a production environment
* upgrade to the latest version of the distribution.
   
   
Grakn releases from our [GitHub repository](https://github.com/graknlabs/grakn) are self-contained packages (tar.gz/zip) containing all the components you need to run Grakn:

* Grakn stack
* Redis
* Cassandra

You can run all these components separately depending on the redundancy levels you require.

## Setting up Grakn

### Minimum Requirements

Grakn can run on default Java settings (heap of 768MB, 1GB machine) if the graph is small enough. Recommended production settings are at least 4GB machine with 3GB heap.

Cassandra will need at least 1GB of memory by itself if run on same machine.

### Standalone Grakn

You can start a standalone instance of Grakn by running `grakn.sh start`. This will produce a working environment for importing and analysing your data.

By default Grakn stores your data in the extracted directory, under `db/cassandra/` in the folder into which you unzipped the distribution zip.

We recommend changing this to another location to make upgrading Grakn easier and faster in the future. Having changed it, you need to specify the new location in the `conf/cassandra/cassandra.yaml` configuration file:

* `data_file_directories`: eg. /var/lib/cassandra/data
* `commitlog_directory`: eg. /var/lib/cassandra/commitlog
* `saved_caches_directory`: eg. /var/lib/cassandra/saved_caches

### Clustered Database

To add reliability and redudancy on the database layer, you can run Cassandra (DSE, OrientDB, other storage backends) separately and point Grakn to use the node/cluster.

Running Cassandra requires an entire guide in itself - our brief recommendation is to have, at the minimum, a cluster of 3 nodes with replication factor (RF) 3.

Cassandra version: 2.1.9

Specify an external database cluster in `conf/main/grakn.properties`: `storage.hostname`: eg. 10.0.10.1,10.0.10.2,10.0.10.3

For further information on running Cassandra, see:

- [Cassandra documentation](http://cassandra.apache.org/doc/latest/operating/index.html)    
- [AWS Cassandra whitepaper](https://d0.awsstatic.com/whitepapers/Cassandra_on_AWS.pdf)

### Distributed

For a fully scalable and distributed setup, you will need to run each component of Grakn in a separate cluster.
This provides the highest level of redundancy and availability; you will want this for high throughput systems and to make the most out of Grakn. 

The database layer will need to be configured similarly to the [Clustered database](#clustered-database) setup.

Grakn Engine uses [Redis](https://redis.io/) for distributed task execution.

Redis version: 3.2.9

Once the configuration files are correct, you can start Grakn Engine only with `bin/grakn-engine.sh start`.
Multiple instances of Grakn Engine can be started that will make use of the shared Cassandra/Redis clusters.

## Upgrading Grakn

### Standalone

#### Default directory
For the default storage directory, `db/cassandra/`, you need to:

- stop old Grakn (`./bin/grakn.sh stop`)
- extract the latest Grakn package into a new directory
- copy the entire contents of the `db` directory to the new location into the new `db` directory
- start new Grakn (`./bin/grakn.sh start`)

#### External directory
If you have changed the location of data_file_directories in the `conf/cassandra/cassandra.yaml`, you need to:

- stop old Grakn (`./bin/grakn.sh stop`)
- extract the latest Grakn package into a new directory
- amend `data_file_directories`, `commitlog_directory` and `saved_caches_directory` to match your custom directories
- start new Grakn (`./bin/grakn.sh start`)

You will need to amend these variables with every new version of Grakn.

### Distributed

Upgrading Grakn in a distributed setup is very simple:

- stop and remove old Grakn Engine (`./bin/grakn-engine.sh stop`)
- roll out the latest Grakn package with the correct Redis variables in the configuration files
- start new Grakn Engine (`./bin/grakn-engine.sh start`)

You can perform a rolling deployment in this fashion with minimum impact on your services.

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/24" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}

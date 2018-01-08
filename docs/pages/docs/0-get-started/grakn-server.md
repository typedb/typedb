---
title: Grakn Deployment Guide
keywords: setup, getting started, download
tags: [getting-started]
summary: "A guide to setting up a production deployment of GRAKN.AI."
sidebar: documentation_sidebar
permalink: /docs/get-started/grakn-server
folder: docs
---


## Introduction

This guide offers advice on how to:

* run Grakn in a production environment
* upgrade to the latest version of the distribution.


Grakn releases from our [GitHub repository](https://github.com/graknlabs/grakn) are self-contained packages (tar.gz/zip) containing all the components you need to run Grakn.

## Setting up Grakn

### Minimum Requirements

Grakn can run on default Java settings (heap of 768MB, 1GB machine) if the knowledge base is small enough.
Recommended production settings are at least 4GB machine with 3GB heap.

### Standalone Grakn

You can start a standalone instance of Grakn by running `grakn server start`. This will produce a working environment for importing and analysing your data.

By default Grakn stores your data in the extracted directory, under `db/cassandra/` in the folder into which you unzipped the distribution zip.

We recommend changing this to another location to make upgrading Grakn easier and faster in the future. Having changed it, you need to specify the new location in the `conf/cassandra/cassandra.yaml` configuration file:

* `data_file_directories`: eg. /var/lib/cassandra/data
* `commitlog_directory`: eg. /var/lib/cassandra/commitlog
* `saved_caches_directory`: eg. /var/lib/cassandra/saved_caches

### Grakn Cluster

Grakn's clustering functionality is part of [Grakn KBMS](https://grakn.ai/kbms).

## Upgrading Grakn

#### Default directory
For the default storage directory, `db/cassandra/`, you need to:

- stop old Grakn (`grakn server stop`)
- extract the latest Grakn package into a new directory
- copy the entire contents of the `db` directory to the new location into the new `db` directory
- start new Grakn (`grakn server start`)

#### External directory
If you have changed the location of data_file_directories in the `conf/cassandra/cassandra.yaml`, you need to:

- stop old Grakn (`grakn server stop`)
- extract the latest Grakn package into a new directory
- amend `data_file_directories`, `commitlog_directory` and `saved_caches_directory` to match your custom directories
- start new Grakn (`grakn server start`)

You will need to amend these variables with every new version of Grakn.

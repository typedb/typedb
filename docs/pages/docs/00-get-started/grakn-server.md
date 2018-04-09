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

## Booting up Grakn

Grakn can run on default Java settings (heap of 768MB, 1GB machine) if the knowledge graph is small enough.
Recommended production settings are at least 4GB machine with 3GB heap.

Start Grakn with:

```
grakn server start
```

By default Grakn stores your data under `$GRAKN_HOME/db/` and log files under `$GRAKN_HOME/logs/`. It can be changed by updating the `data-dir` and `log.dirs` properties within `conf/grakn.properties`, respectively.

For production use, we highly recommend changing them to an **external directory** outside of `$GRAKN_HOME` in order to make upgrading Grakn easier in the future.

In order to benefit from maximum performance, housing the data should in a fast SSD drive is ideal.

### Grakn Cluster

Grakn's clustering functionality is part of [Grakn KGMS](https://grakn.ai/grakn-kgms).

## Upgrading Grakn

#### Default directory
For the default storage and logs directory, ie., `$GRAKN_HOME/db/` and `$GRAKN_HOME/logs/`, you need to:

- Stop the current Grakn if it is running (`grakn server stop`)
- Extract the latest Grakn package into a new directory
- Copy the entire contents of the `db` directory to the new location into the new `db` directory
- Copy `conf/grakn.properties` from the current Grakn into the new Grakn
- Start the new Grakn (`grakn server start`)

For production use, we highly recommend changing them to an **external directory** outside of `$GRAKN_HOME` in order to make upgrading Grakn easier in the future.

#### External directory
If you have changed the location of  in the `grakn.properties`, you need to:

- Stop the current Grakn if it is running (`grakn server stop`)
- Extract the latest Grakn package into a new directory
- Copy `conf/grakn.properties` from the current Grakn into the new Grakn
- Start the new Grakn (`grakn server start`)
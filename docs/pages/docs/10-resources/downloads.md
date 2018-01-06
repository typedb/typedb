---
title: Downloads
keywords: setup, getting started, download
tags: [getting-started]
summary: "
This is a list of Grakn releases. It's the place to come to download the most recent versions of Grakn."
sidebar: documentation_sidebar
permalink: /docs/resources/downloads
folder: docs
---


## Download Grakn Latest Version

[Download Grakn 1.0](https://grakn.ai/download/latest)
{: #download-btn }


### Past Versions
A list of previously released versions of Grakn can be found on [Github](https://grakn.ai/download).


## Prerequisites

{% include note.html content="We currently support Mac OS X and Linux. Grakn requires Java 8 (OpenJDK or Oracle) with the `$JAVA_HOME` set accordingly. Grakn also requires Maven 3." %}

## Code
We are an open source project. If you want to look at our code, we are on Github at [https://github.com/graknlabs/grakn](https://github.com/graknlabs/grakn).

### Building the Code

To build Grakn, you need Maven, node.js and npm. Make sure you have the most recent version of node.js and npm.

Using git, clone the [Grakn repository](https://github.com/graknlabs/grakn) to a local directory.  In that directory:

```bash
mvn package -DskipTests
```

When the build has completed, you will find it in the `grakn-dist` directory under `target`. The zip file built into that directory is the same as that distributed as a release on [Github](https://grakn.ai/download).

### Example Code
You can find an additional repo on Github containing our [example code](https://github.com/graknlabs/sample-projects), while further information about the examples is [here](../examples/examples-overview).


## Questions?
Please see our [FAQ](../resources/faq) page if you encounter any problems when installing and running Grakn. If our guide doesn't cover the issue, please do get in touch on our [discussion forums](http://discuss.grakn.ai), on [Stack Overflow](http://www.stackoverflow.com) or via our [Slack channels](https://grakn.ai/slack.html).

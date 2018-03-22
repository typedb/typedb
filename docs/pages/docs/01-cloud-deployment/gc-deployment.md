---
title: Google Cloud Deployment
keywords: cloud, deployment, google
tags: [getting-started, deployment, cloud]
summary: "Deploying Grakn on Google Cloud"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/gc-deployment
folder: docs
---

# Google Cloud Launcher

## Deployment
We shall begin with deployment of a Grakn cluster. The procedure is straight-forward and takes advantage of the
[Cloud Launcher](https://console.cloud.google.com/launcher/details/datastax-public/datastax-enterprise):

![](/images/gc-solution-listing.png)

To start deployment, click on the "Launch on Compute Engine" button which will take you to the configuration screen

![](/images/gc-deployment-options.png)

Feel free to adjust the settings to your needs. When satisfied with the configuration press "Deploy"

![](/images/gc-deployment-pending.png)

That is all! Your cluster deployment is now pending.

When the deployment is complete you should be able to see the post-deployment screen:

![](/images/gc-deployment-complete.png)

## Accessing the Grakn Dashboard

In order to access the Grakn Dashboard, you can either allow traffic on the 4567 TCP port or create an ssh tunnel.

#### Adding firewall rule
Unless you selected the `Allow TCP port 4567 traffic` option during deployment, you can allow traffic by running the command in red circle in your terminal:

![](/images/gc-firewall-command.png)

After allowing the traffic on TCP port 4567 the Dashboard shall be accessible from your browser at <node-external-ip>:4567.

#### Creating SSH tunnel
To create a SSH tunnel, copy & paste the black box inside the red oval to your terminal:

![](/images/gc-ssh-tunnel-command.png)

After executing the command the output should look like this:

![](/images/gc-ssh-tunnel-terminal.png)

After SSH tunnel creation, the Dashboard shall be accessible from your browser at [localhost](https://localhost:8443).

## Next Steps

If you want to learn more about Grakn KGMS, the [Grakn Academy](https://dev.grakn.ai/academy/) is a good place to start.

To learn more about running Grakn KGMS on GCP take a look at the [best practices guide](best-practices.md) and [post deployment steps](post-deployment-steps.md).

---
title: Post Deployment
keywords: cloud, deployment, google
tags: [getting-started, deployment, cloud]
summary: "Post Deployment Steps"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/post-deployment
folder: docs
---

# Post Deployment

## Security

The default security settings of Google Cloud Platform offer reasonable amount of security. As the global network is flat, it necessitates in all traffic going over private IPs. As a result of that and the fact that by default all GC deployments are sealed of from external access, significantly less amount of work is needed for setting firewall rules than on other platforms.tThe SSH access to machines either needs to be authenticated via console or CLI.

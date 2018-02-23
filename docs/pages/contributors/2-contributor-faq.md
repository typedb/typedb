---
title: Contributor FAQ
keywords: contributors

summary: This is an FAQ for developers working on the GRAKN.AI platform
tags: [overview]
sidebar: contributors_sidebar
permalink: ./contributors/contributor-faq.html
folder: overview
---

## Introduction

## General Questions

### How can I contribute to GRAKN.AI?

We are happy to receive contributions of platform or test code, example projects, bug reports, support to other community members, or documentation (fixes or translation). 


## Developer FAQs

This section records technical issues that you may hit upon if you're working on a bug fix or feature request in the GRAKN.AI codebase (that is, working on the GRAKN.AI platform). Feel free to edit the page (or suggest edits via Slack) if you have a 'favourite' issue to share with other contributing developers!

### How do I resolve this weird exception?

I used to be able to start GraknEngineServer, but now I get this weird exception when running running the `main` in `GraknEngineServer`. Stack Overflow recommends excluding the Java servlet API for Apache spark, but then those classes are not provided from elsewhere. 

```
Exception in thread "Thread-0" java.lang.ExceptionInInitializerError
    at spark.embeddedserver.jetty.EmbeddedJettyFactory.create(EmbeddedJettyFactory.java:34)
    at spark.embeddedserver.EmbeddedServers.create(EmbeddedServers.java:57)
    at spark.Service.lambda$init$1(Service.java:384)
    at spark.Service$$Lambda$3/302155142.run(Unknown Source)
    at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.SecurityException: class "javax.servlet.http.HttpSessionIdListener"'s signer information does not match signer information of other classes in the same package
    at java.lang.ClassLoader.checkCerts(ClassLoader.java:895)
    at java.lang.ClassLoader.preDefineClass(ClassLoader.java:665)
    at java.lang.ClassLoader.defineClass(ClassLoader.java:758)
    at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
    at java.net.URLClassLoader.defineClass(URLClassLoader.java:467)
    at java.net.URLClassLoader.access$100(URLClassLoader.java:73)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:368)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:362)
    at java.security.AccessController.doPrivileged(Native Method)
    at java.net.URLClassLoader.findClass(URLClassLoader.java:361)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
    at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:331)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
    at org.eclipse.jetty.server.session.SessionHandler.<clinit>(SessionHandler.java:54)
    ... 5 more
```	

**How did I resolve it?**

The solution was to include the Janus factory dependency explicitly:

```bash
<dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-factory</artifactId>
    <version>${project.version}</version>
</dependency>
```
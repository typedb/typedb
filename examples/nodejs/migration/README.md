---
title: Loading CSV, JSON and XML data into Grakn - an Example
keywords: migration, csv to grakn, json to grakn, xml to grakn, nodejs client
tags: [example]
sidebar: documentation_sidebar
permalink: /examples/nodejs/migration
folder: examples
symlink: false
---

## Loading CSV, JSON and XML data into Grakn - an Example

These examples uses the [Grakn Node.js Client](https://github.com/graknlabs/grakn/tree/master/client-nodejs) to load a dataset in CSV, JSON and XML formats into a Grakn keyspace.

### Prerequisites

- Grakn 1.3.0 && Node >= 6.5.0
- Basic understading of [GRAKN.AI](http://dev.grakn.ai/docs)
- Basic knowledge of Javascript and Node.js

### Understanding the code

- Read the **[blog series](https://medium.com/@soroush_26094/load-csv-json-and-xml-data-into-grakn-1ab5bf70348)**
- Read the comments in `index.js`

### Quick start

1. [Load the schema into the _phone_calls_ Grakn keyspace](https://medium.com/@soroush_26094/modelling-simple-grakn-schema-7fbac77bfcf5)
2. cd into `/csv`, `/json` or `/xml`
3. Run `npm install`
4. Run `npm run start`

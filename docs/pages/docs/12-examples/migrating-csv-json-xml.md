---
title: Migrating CSV, JSON and XML Data
keywords: migration, csv, json, xml, knowledge graph
tags: [migration, csv, json, xml, knowledge graph]
summary: "How to migrate csv, json and xml data to a Grakn knolwedge graph"
sidebar: documentation_sidebar
permalink: /docs/examples/migrating-csv-json-xml
folder: docs
toc: false
---

## Goal

In this tutorial, our aim is to migrate some actual data to the `phone_calls` knowledge graph. We defined this schema previously, in the **[Defining the Schema](/defining-the-schema)** section.

## A Quick Look at the Schema

Before we get started with migration, let’s have a quick reminder of how the schema for the `phone_calls` knowledge graph looks like.

![The Visualised Schema](/images/examples-migrating-csv-json-xml-1.png)

## Python or Node.js?

Pick a language of your choice to continue.

<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active"><a href="#overview-python" data-toggle="tab">Python</a></li>
    <li><a href="#overview-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="overview-python">
    <!-- python: an overview -->
    <h2>An Overview</h2>
    <p>Let’s go through a summary of how the migration takes place.</p>
    <ol>
      <li><p>we need a way to talk to our Grakn keyspace. To do this, we will use the <a href="https://github.com/graknlabs/grakn/tree/master/client-python">Python Client</a>.</p></li>
      <li><p>we will go through each data file, extracting each data item and parsing it to a Python dictionary.</p></li>
      <li><p>we will pass each data item (in the form of a Python dictionary) to its corresponding template function, which in turn gives us the constructed Graql query for inserting that item into Grakn.</p></li>
      <li><p>we will execute each of those queries to load the data into our target keyspace — phone_calls.</p></li>
    </ol>
    <p>Before moving on, make sure you have <strong>Python3</strong> and <strong>Pip3</strong> installed and the <a href="http://dev.grakn.ai/docs/get-started/grakn-server"><strong>Grakn server</strong></a> running on your machine.</p>
    <!-- python: getting started -->
    <h2 id="gettingstarted">Getting Started</h2>
    <ol>
      <li><p>Create a directory named <code>phone_calls</code> on your desktop.</p></li>
      <li><p>cd to the phone_calls directory via terminal.</p></li>
      <li><p>Run <code>pip3 install grakn</code> to install the <a href="https://github.com/graknlabs/grakn/tree/master/client-python">Grakn Python Client</a>.</p></li>
      <li><p>Open the <code>phone_calls</code> directory in your favourite text editor.</p></li>
      <li><p>Create a <code>migrate.py</code> file in the root directory. This is where we’re going to write all our code.</p></li>
    </ol>
    <!-- python: including the data files -->
    <h2 id="includingthedatafiles">Including the Data Files</h2>
    <p>Pick one of the data formats below and download the files. After you download them, place the four files under the <code>phone_calls/data directory</code>. We will be using these to load their data into our <code>phone_calls</code> knowledge graph.</p>
    <p><strong>CSV:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/companies.csv">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/people.csv">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/contracts.csv">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/calls.csv">calls</a></p>
    <p><strong>JSON:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/companies.json">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/people.json">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/contracts.json">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/calls.json">calls</a></p>
    <p><strong>XML:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/companies.xml">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/people.xml">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/contracts.xml">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/calls.xml">calls</a></p>
    <!-- python: setting up the migration mechanism -->
    <h2 id="settingupthemigrationmechanism">Setting up the migration mechanism</h2>
    <p>All code that follows is to be written in <code>phone_calls/migrate.py</code>.</p>
    {% gist 59cee5dea1fc1e10cbaf2e70de56ef9c %}
    <p>First thing first, we import the grakn module. We will use it for connecting to our <code>phone_calls</code> keyspace.</p>
    <p>Next, we declare the <code>inputs</code>. More on this later. For now, what we need to understand about inputs — it’s a list of dictionaries, each one containing:</p>
    <ul>
      <li><p>The path to the data file</p></li>
      <li><p>The template function that receives a dictionary and produces the Graql insert query. We will define these template functions in a bit.</p></li>
    </ul>
    <p>Let’s move on.</p>
    <!-- python: build_phone_call_graph(inputs) -->
    <h2 id="build_phone_call_graphinputs">build_phone_call_graph(inputs)</h2>
    {% gist 59cee5dea1fc1e10cbaf2e70de56ef9c %}
    <p>This is the main and only function we need to call to start loading data into Grakn.</p>
    <p>What happens in this function, is as follows:</p>
    <ol>
      <li><p>A Grakn <code>client</code> is created, connected to the server we have running locally.</p></li>
      <li><p>A <code>session</code> is created, connected to the keyspace <code>phone_calls</code>. Note that by using <code>with</code>, we indicate that the session will close after it’s been used.</p></li>
      <li><p>For each <code>input</code> dictionary in <code>inputs</code>, we call the <code>load_data_into_grakn(input, session)</code>. This will take care of loading the data as specified in the input dictionary into our keyspace.</p></li>
    </ol>
    <!-- python: load_data_into_grakn(inputs) -->
    <h2 id="load_data_into_grakninputsession">load_data_into_grakn(input, session)</h2>
    {% gist 2b4e6f3d5d88fddc72f6536959a75f1e %}
    <p>In order to load data from each file into Grakn, we need to:</p>
    <ol>
      <li><p>retrieve a list containing dictionaries, each of which represents a data item. We do this by calling <code>parse_data_to_dictionaries(input)</code></p></li>
      <li><p>for each dictionary in <code>items</code>: a) create a transaction <code>tx</code>, which closes once used, b) construct the <code>graql_insert_query</code> using the corresponding template function, c) execute the query and d)commit the transaction.</p></li>
    </ol>
    {% include tip.html content="to avoid running out of memory, it’s recommended that every single query gets created and committed in a single transaction. However, for faster migration of large datasets, this can happen once for every <code>n</code> queries, where <code>n</code> is the maximum number of queries guaranteed to run on a single transaction." %}
    <p>Before we move on to parsing the data into dictionaries, let’s start with the template functions.</p>
    <!-- python: the template functions -->
    <h2 id="thetemplatefunctions">The Template Functions</h2>
    <p>Templates are simple functions that accept a dictionary, representing a single data item. The values within this dictionary fill in the blanks of the query template. The result will be a Graql insert query.</p>
    <p>We need 4 of them. Let’s go through them one by one.</p>
    <!-- python: companyTemplate -->
    <h3 id="companytemplate">companyTemplate</h3>
    {% gist f3f84ec40f1eb59c556aca1f9f1cb871 %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ name: "Telecom" }</code></p></li>
      <li><p><code>Comes out: insert $company isa company has name "Telecom";</code></p></li>
    </ul>
    <!-- python: personTemplate -->
    <h3 id="persontemplate">personTemplate</h3>
    {% gist ce89b46e35727c71d598aff1103d765f %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ phone_number: "+44 091 xxx" }</code></p></li>
      <li><p>Comes out: <code>insert $person has phone-number "+44 091 xxx";</code></p></li>
    </ul>
    <p>or:</p>
    <ul>
      <li><p>Goes in: <code>{ firs-name: "Jackie", last-name: "Joe", city: "Jimo", age: 77, phone_number: "+00 091 xxx"}</code></p></li>
      <li><p>Comes out: <code>insert $person has phone-number "+44 091 xxx" has first-name "Jackie" has last-name "Joe" has city "Jimo" has age 77;</code></p></li>
    </ul>
    <!-- python: contractTemplate -->
    <h3 id="contracttemplate">contractTemplate</h3>
    {% gist 07a93efd27a977342c668fb659c18d2f %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ company_name: "Telecom", person_id: "+00 091 xxx" }</code></p></li>
      <li><p>Comes out: <code>match $company isa company has name "Telecom"; $customer isa person has phone-number "+00 091 xxx"; insert (provider: $company, customer: $customer) isa contract;</code></p>
      </li>
    </ul>
    <!-- python: callTemplate -->
    <h3 id="calltemplate">callTemplate</h3>
    {% gist 315d7c04c922a4c1fea4e925531b89cd %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ caller_id: "+44 091 xxx", callee_id: "+00 091 xxx", started_at: 2018–08–10T07:57:51, duration: 148 }</code></p></li>
      <li><p>Comes out: match <code>$caller isa person has phone-number "+44 091 xxx"; $callee isa person has phone-number "+00 091 xxx"; insert $call(caller: $caller, callee: $callee) isa call; $call has started-at 2018–08–10T07:57:51; $call has duration 148;</code></p></li>
    </ul>
    <p>We’ve now created a template for each and all four concepts that were <a href="./defining-the-schema">previously</a> defined in the schema.</p>
    <p>It’s time for the implementation of <code>parse_data_to_dictionaries(input)</code>.</p>
    <!-- python: dataformat-specific implementation -->
    <h2 id="dataformatspecificimplementation">DataFormat-specific implementation</h2>
    <p>The implementation for <code>parse_data_to_dictionaries(input)</code> differs based on what format our data files have.</p>
    <p><code>.csv</code>, <code>.json</code> or <code>.xml</code>.</p>
    <!-- tabs for csv, json and xml parsing implementation -->
    <ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
      <li class="active" style="width: 33% !important"><a href="#python-format-csv" data-toggle="tab">CSV</a></li>
      <li style="width: 33% !important"><a href="#python-format-json" data-toggle="tab">JSON</a></li>
      <li style="width: 34% !important"><a href="#python-format-xml" data-toggle="tab">XML</a></li>
    </ul>
    <div class="tab-content tab-content--intro">
      <!-- csv implementation -->
      <div role="tabpanel" class="tab-pane active" id="python-format-csv">
        <p>We will use  Python’s built-in <a href="https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters"><code>csv</code> library</a>. Let’s import the module for it.</p>
        {% gist dd4c57c1fde706a96b87b6f2487c828c %}
        <p>Moving on, we will write the implementation of <code>parse_data_to_dictionaries(input)</code> for parsing <code>.csv</code> files. Note that we use <a href="https://docs.python.org/3/library/csv.html#csv.DictReader">DictReader</a> to map the information in each row to a dictionary.</p>
        {% gist 59d4bc7dac18bf95139ad75b5ade8a71 %}
        <p>Besides this function, we need to make one more change.</p>
        <p>Given the nature of CSV files, the dictionary produced will have all the columns of the <code>.csv</code> file as its keys, even when the value is not there, it’ll be taken as a blank string.</p>
        <p>For this reason, we need to change one line in our <code>person_template</code> function.</p>
        <p><code>if "first_name" in person</code> becomes <code>if person["first_name"] == ""</code>.</p>
      </div>
      <!-- json implementation -->
      <div role="tabpanel" class="tab-pane" id="python-format-json">
        <p>We will use <a href="https://pypi.org/project/ijson/">ijson</a>, an iterative JSON parser with a standard Python iterator interface.</p>
        <p>Via the terminal, while in the <code>phone_calls</code> directory, run <code>pip3 install ijson</code> and import the module for it.</p>
        {% gist 3590355e4bbe8dde03778dacb6e07d33 %}
        <p>Moving on, we will write the implementation of <code>parse_data_to_dictionaries(input)</code> for processing <code>.json</code> files.</p>
        {% gist 3d2b4e6889cbd6e2aa8e149ca7a8fe49 %}
      </div>
      <!-- xml implementation -->
      <div role="tabpanel" class="tab-pane" id="python-format-xml">
        <p>We will use Python’s built-in <a href="https://docs.python.org/2/library/xml.etree.elementtree.html"><code>xml.etree.cElementTree</code> library</a>. Let’s import the module for it.</p>
        {% gist 330947fda05bb1cebb326fd8bcd911f6 %}
        <p>For parsing XML data, we need to know the target tag name. This needs to be specified for each data file in our <code>inputs</code> deceleration.</p>
        {% gist 74ac2de6575f5b4e008cbc0ee845031f %}
        <p>And now for the implementation of <code>parse_data_to_dictionaries(input)</code> for parsing <code>.xml</code> files.</p>
        <p>The implementation below, although, not the most generic, performs well with very large <code>.xml</code> files. Note that many libraries that do xml to dictionary parsing, pull in the entire <code>.xml</code> file into memory first. There is nothing wrong with that approach when you’re dealing with small files, but when it comes to large files, that’s just a no go.</p>
        {% gist e67e5003de2a8753dd2f91e249dc69b7 %}
      </div>
    </div>
    <!-- putting it all together -->
    <h2>Putting it all together</h2>
    Here is how our <code>migrate.js</code> looks like for each data format.
    <!-- tabs for csv, json and xml migrate.py -->
    <ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
      <li class="active" style="width: 33% !important"><a href="#python-complete-csv" data-toggle="tab">CSV</a></li>
      <li style="width: 33% !important"><a href="#python-complete-json" data-toggle="tab">JSON</a></li>
      <li style="width: 34% !important"><a href="#python-complete-xml" data-toggle="tab">XML</a></li>
    </ul>
    <div class="tab-content tab-content--intro">
      <!-- csv complete implementation -->
      <div role="tabpanel" class="tab-pane active" id="python-complete-csv">
        {% gist 7508b1a093b60caad0fc83624cf7da42 %}
      </div>
      <!-- json complete implementation -->
      <div role="tabpanel" class="tab-pane" id="python-complete-json">
        {% gist edd1e19bd958d86e1dc3b0c779247880 %}
      </div>
      <!-- xml complete implementation -->
      <div role="tabpanel" class="tab-pane" id="python-complete-xml">
        {% gist 097ff583ab9d074b62a30945251087d4 %}
      </div>
    </div>
    <!-- time to lead -->
    <h2 id="timetoload">Time to Load</h2>
    <p>Run <code>python3 migrate.py</code></p>
    <p>Sit back, relax and watch the logs while the data starts pouring into Grakn.</p>
    <!-- ... so far with the migration -->
    <h3 id="sofarwiththemigration">... so far with the migration</h3>
    <p>We started off by setting up our project and positioning the data files.</p>
    <p>Next we went on to set up the migration mechanism, one that was independent of the data format.</p>
    <p>Then, we went ahead and wrote the template functions whose only job was to construct a Graql insert query based on the data passed to them.</p>
    <p>After that, we learned how files with different data formats can be parsed into Python dictionaries.</p>
    <p>Lastly, we ran <code>python3 migrate.py</code> which fired the <code>build_phone_call_graph</code> function with the given <code>inputs</code>. This loaded the data into our Grakn knowledge graph.</p>
  </div>

  <div role="tabpanel" class="tab-pane" id="overview-nodejs">
    <!-- nodejs: an overview -->
    <h2 id="anoverview">An Overview</h2>
    <p>Let’s go through a summary of how the migration takes place.</p>
    <ol>
      <li><p>we need a way to talk to our Grakn keyspace. To do this, we will use the <a href="https://github.com/graknlabs/grakn/tree/master/client-nodejs">Node.js Client</a>.</p></li>
      <li><p>we will go through each data file, extracting each data item and parsing it to a Javascript object.</p></li>
      <li><p>we will pass each data item (in the form of a Javascript object) to its corresponding template function, which in turn gives us the constructed Graql query for inserting that item into Grakn.</p></li>
      <li><p>we will execute each of those queries to load the data into our target keyspace — phone_calls.</p></li>
    </ol>
    <p>Before moving on, make sure you have <strong>npm</strong> installed and the <a href="http://dev.grakn.ai/docs/get-started/grakn-server"><strong>Grakn server</strong></a> running on your machine.</p>
    <!-- nodejs: getting started -->
    <h2 id="gettingstarted">Getting Started</h2>
    <ol>
      <li><p>Create a directory named <code>phone_calls</code> on your desktop.</p></li>
      <li><p>cd to the <code>phone_calls</code> directory via terminal.</p></li>
      <li><p>Run <code>npm install grakn</code> to install the <a href="https://github.com/graknlabs/grakn/tree/master/client-nodejs">Grakn Node.js Client</a>.</p></li>
      <li><p>Open the <code>phone_calls</code> directory in your favourite text editor.</p></li>
      <li><p>Create a <code>migrate.js</code> file in the root directory. This is where we’re going to write all our code.</p></li>
    </ol>
    <!-- nodejs: including the data files -->
    <h2 id="includingthedatafiles">Including the Data Files</h2>
    <p>Pick one of the data formats below and download the files. After you download them, place the four files under the <code>phone_calls/data</code> directory. We will be using these to load their data into our <code>phone_calls</code> knowledge graph.</p>
    <p><strong>CSV:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/companies.csv">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/people.csv">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/contracts.csv">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/csv/data/calls.csv">calls</a></p>
    <p><strong>JSON:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/companies.json">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/people.json">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/contracts.json">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/json/data/calls.json">calls</a></p>
    <p><strong>XML:</strong> <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/companies.xml">companies</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/people.xml">people</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/contracts.xml">contracts</a> | <a href="https://raw.githubusercontent.com/graknlabs/examples/master/nodejs/migration/xml/data/calls.xml">calls</a></p>
    <!-- nodejs: setting up the migration mechanism -->
    <h2 id="settingupthemigrationmechanism">Setting up the migration mechanism</h2>
    <p>All code that follows is to be written in <code>phone_calls/migrate.js</code>.</p>
    {% gist 50e72ea0110190eb3fdb16db516b0f01 %}
    <p>First thing first, we require the grakn module. We will use it for connecting to our <code>phone_calls</code> keyspace.</p>
    <p>Next, we declare the <code>inputs</code>. More on this later. For now, what we need to understand about inputs — it’s an array of objects, each one containing:</p>
    <ul>
      <li><p>The path to the data file</p></li>
      <li><p>The template function that receives an object and produces the Graql insert query. We will define these template functions in a bit.</p></li>
    </ul>
    <p>Let’s move on.</p>
    <!-- nodejs: buildPhoneCallGraph(inputs) -->
    <h2 id="buildPhoneCallGraphinputs">buildPhoneCallGraph(inputs)</h2>
    {% gist c0c7463f3077eead80bc169bfa837aff %}
    <p>This is the main and only function we need to call to start loading data into Grakn.</p>
    <p>What happens in this function, is as follows:</p>
    <ol>
      <li><p>A <code>grakn</code> instance is created, connected to the server we have running locally.</p></li>
      <li><p>A <code>session</code> is created, connected to the keyspace <code>phone_calls</code>.</p></li>
      <li><p>For each <code>input</code> object in <code>inputs</code>, we call the <code>loadDataIntoGrakn(input, session)</code>. This will take care of loading the data as specified in the input object into our keyspace.</p></li>
      <li><p>The <code>session</code> is closed.</p></li>
    </ol>
    <!-- nodejs: loadDataIntoGrakn(input, session) -->
    <h2 id="loaddataintograkninputsession">loadDataIntoGrakn(input, session)</h2>
    {% gist b57395a4cae644439ac5d0e573d0e49b %}
    <p>In order to load data from each file into Grakn, we need to:</p>
    <ol>
      <li><p>retrieve a list containing objects, each of which represents a data item. We do this by calling <code>parseDataToObjects(input)</code></p></li>
      <li><p>for each object in <code>items</code>: a) create a transaction <code>tx</code>, b) construct the <code>graqlInsertQuery</code> using the corresponding template function, c) run the query and d)commit the transaction.</p></li>
    </ol>
    {% include tip.html content="to avoid running out of memory, it’s recommended that every single query gets created and committed in a single transaction. However, for faster migration of large datasets, this can happen once for every <code>n</code> queries, where <code>n</code> is the maximum number of queries guaranteed to run on a single transaction." %}
    <p>Before we move on to parsing the data into objects, let’s start with the template functions.</p>
    <!-- nodejs: the template functions -->
    <h2 id="thetemplatefunctions">The Template Functions</h2>
    <p>Templates are simple functions that accept an object, representing a single data item. The values within this object fill in the blanks of the query template. The result will be a Graql insert query.</p>
    <p>We need 4 of them. Let’s go through them one by one.</p>
    <!-- nodejs: companyTemplate -->
    <h3 id="companytemplate">companyTemplate</h3>
    {% gist 213c3a5100ad0e361abb545d7a252cf1 %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ name: "Telecom" }</code></p></li>
      <li><p><code>Comes out: insert $company isa company has name "Telecom";</code></p></li>
    </ul>
    <!-- nodejs: personTemplate -->
    <h3 id="persontemplate">personTemplate</h3>
    {% gist c496df88af2b4024206ab7c27b5d2bd4 %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ phone_number: "+44 091 xxx" }</code></p></li>
      <li><p>Comes out: <code>insert $person has phone-number "+44 091 xxx";</code></p></li>
    </ul>
    <p>or:</p>
    <ul>
      <li><p>Goes in: <code>{ firs-name: "Jackie", last-name: "Joe", city: "Jimo", age: 77, phone_number: "+00 091 xxx"}</code></p></li>
      <li><p>Comes out: <code>insert $person has phone-number "+44 091 xxx" has first-name "Jackie" has last-name "Joe" has city "Jimo" has age 77;</code></p></li>
    </ul>
    <!-- nodejs: contractTemplate -->
    <h3 id="contracttemplate">contractTemplate</h3>
    {% gist 0b4461cc6dd05e75e3ba3dc68b85cb10 %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ company_name: "Telecom", person_id: "+00 091 xxx" }</code></p></li>
      <li><p>Comes out: <code>match $company isa company has name "Telecom"; $customer isa person has phone-number "+00 091 xxx"; insert (provider: $company, customer: $customer) isa contract;</code></p>
      </li>
    </ul>
    <!-- nodejs: callTemplate -->
    <h3 id="calltemplate">callTemplate</h3>
    {% gist 80273905e71b6e5d5f1815c32cee322a %}
    <p>Example:</p>
    <ul>
      <li><p>Goes in: <code>{ caller_id: "+44 091 xxx", callee_id: "+00 091 xxx", started_at: 2018–08–10T07:57:51, duration: 148 }</code></p></li>
      <li><p>Comes out: match <code>$caller isa person has phone-number "+44 091 xxx"; $callee isa person has phone-number "+00 091 xxx"; insert $call(caller: $caller, callee: $callee) isa call; $call has started-at 2018–08–10T07:57:51; $call has duration 148;</code></p></li>
    </ul>
    <p>We’ve now created a template for each and all four concepts that were <a href="./defining-the-schema">previously</a> defined in the schema.</p>
    <p>It’s time for the implementation of <code>parseDataToObjects(input)</code>.</p>
    <!-- nodejs: data-specific implementation -->
    <h2 id="dataformatspecificimplementation">DataFormat-specific implementation</h2>
    <p>The implementation for parseDataToObjects(input) differs based on what format our data files have.</p>
    <p><code>.csv</code>, <code>.json</code> or <code>.xml</code>.</p>
    <!-- tabs for csv, json and xml parsing implementation -->
    <ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
      <li class="active" style="width: 33% !important"><a href="#nodejs-format-csv" data-toggle="tab">CSV</a></li>
      <li style="width: 33% !important"><a href="#nodejs-format-json" data-toggle="tab">JSON</a></li>
      <li style="width: 34% !important"><a href="#nodejs-format-xml" data-toggle="tab">XML</a></li>
    </ul>
    <div class="tab-content tab-content--intro">
      <!-- csv implementation -->
      <div role="tabpanel" class="tab-pane active" id="nodejs-format-csv">
      <p>We will use <a href="https://www.papaparse.com/">Papaparse</a>, a CSV (or delimited text) parser.</p>
      <p>Via the terminal, while in the <code>phone_calls</code> directory, run <code>npm install papaparse</code> and require the module for it.</p>
      {% gist 6324e74376e3306718a9c0ad72eb08ac %}
      <p>Moving on, we will write the implementation of <code>parseDataToObjects(input)</code> for parsing <code>.csv</code> files.</p>
      {% gist aa3952915f08421549cd88b7ced772e1 %}
      <p>Besides this function, we need to make one more change.</p>
      <p>Given the nature of CSV files, the object produced will have all the columns of the <code>.csv</code> file as its keys, even when the value is not there, it’ll be taken as a blank string.</p>
      <p>For this reason, we need to change one line in our person_template function.</p>
      <p><code>const isNotCustomer = typeof first_name === "undefined";</code></p>
      <p>becomes</p>
      <p><code>const isNotCustomer = first_name === “”;</code></p>
      </div>
      <!-- json implementation -->
      <div role="tabpanel" class="tab-pane" id="nodejs-format-json">
        <p>We will use <a href="https://github.com/uhop/stream-json">stream-json</a> for custom JSON processing pipelines with a minimal memory footprint.</p>
        <p>Via the terminal, while in the <code>phone_calls</code> directory, run <code>npm install stream-json</code> and require the modules for it.</p>
        {% gist b1d24637d1ee3b95d0a0a8fb1ce1e086 %}
        <p>Moving on, we will write the implementation of <code>parseDataToObjects(input)</code> for processing <code>.json</code> files.</p>
        {% gist 9c00cf619d6fabff3964a99bbdd659da %}
      </div>
      <!-- xml implementation -->
      <div role="tabpanel" class="tab-pane" id="nodejs-format-xml">
        <p>We will use xml-stream, an xml stream parser.</p>
        <p>Via the terminal, while in the <code>phone_calls</code> directory, run <code>npm install xml-stream</code> and require the module for it.</p>
        {% gist d9928eec3c8b04278e35da94bd7b6519 %}
        <p>For parsing XML data, we need to know the target tag name. This needs to be specified for each data file in our <code>inputs</code> deceleration.</p>
        {% gist e635a264c53de6692cecc5fadf99b1f5 %}
        <p>And now for the implementation of <code>parseDataToObjects(input)</code> for parsing <code>.xml</code> files.</p>
        {% gist ceb90f5ba2a5c40eff6d376ba0f26de3 %}
      </div>
    </div>
    <!-- putting it all together -->
    <h2>Putting it all together</h2>
    Here is how our <code>migrate.js</code> looks like for each data format.
    <!-- tabs for csv, json and xml migrate.js -->
    <ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
      <li class="active" style="width: 33% !important"><a href="#nodejs-complete-csv" data-toggle="tab">CSV</a></li>
      <li style="width: 33% !important"><a href="#nodejs-complete-json" data-toggle="tab">JSON</a></li>
      <li style="width: 34% !important"><a href="#nodejs-complete-xml" data-toggle="tab">XML</a></li>
    </ul>
    <div class="tab-content tab-content--intro">
      <!-- csv complete implementation -->
      <div role="tabpanel" class="tab-pane active" id="nodejs-complete-csv">
        {% gist 709a6afadd4f4f3b319266206ba1fbea %}
      </div>
      <!-- json complete implementation -->
      <div role="tabpanel" class="tab-pane" id="nodejs-complete-json">
        {% gist 0a02bb8cb4b4f00cbbc89b9388da3c86 %}
      </div>
      <!-- xml complete implementation -->
      <div role="tabpanel" class="tab-pane" id="nodejs-complete-xml">
        {% gist 11e7372deefc6b4c05629ec1d2d49ca8 %}
      </div>
    </div>
    <!-- nodejs: time to load -->
    <h2 id="timetoload">Time to Load</h2>
    <p>Run <code>npm run migrate.js</code></p>
    <p>Sit back, relax and watch the logs while the data starts pouring into Grakn.</p>
    <!-- node.js ... so far with the migration -->
    <h3 id="sofarwiththemigration">… so far with the migration</h3>
    <p>We started off by setting up our project and positioning the data files.</p>
    <p>Next we went on to set up the migration mechanism, one that was independent of the data format.</p>
    <p>Then, we went ahead and wrote a template function for each concept. A template’s sole purpose was to construct a Graql insert query for each data item.</p>
    <p>After that, we learned how files with different data formats can be parsed into Javascript objects.</p>
    <p>Lastly, we ran <code>npm run migrate.js</code> which fired the <code>buildPhoneCallGraph</code> function with the given <code>inputs</code>. This loaded the data into our Grakn knowledge graph.</p>
  </div>

</div>
<br />

## Next

Now that we have some actual data in our knowledge graph, we can go ahead and query for insights.

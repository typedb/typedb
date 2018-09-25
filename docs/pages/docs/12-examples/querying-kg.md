---
title: Querying the Knowledge Graph
keywords: query, graql, insights, knowledge graph
tags: [query, graql, insights, knowledge graph]
summary: "How to query a knowledge graph for insights"
sidebar: documentation_sidebar
permalink: /docs/examples/querying-kg
folder: docs
toc: false
---

## Goal

When we [modelled and loaded the schema into Grakn](./defining-the-schema), we had some insights in mind that we wanted to obtain from `phone_calls`; the knowledge graph.

Let’s revise:

- Since September 14th, which customers called person X?

- Who are the people who have received a call from a London customer aged over 50 who has previously called someone aged under 20?

- Who are the common contacts of customers X and Y?

- Who are the customers who 1) have all called each other and 2) have all called person X at least once?

- How does the average call duration among customers aged under 20 compare those aged over 40?

For the rest of this post, we will go through each of these questions to:

- understand their business value,

- write them as a statement,

- write them in [Graql](http://dev.grakn.ai/academy/graql-intro.html), and

- assess their result.

Make sure you have the [Visualisation Dashboard](http://dev.grakn.ai/docs/visualisation-dashboard/visualiser) (at [localhost:4567](http://localhost:4567/)) opened in your browser, while phone_calls selected as the keyspace (in the top-right hand corner).

Let’s begin.

## Since September 14th, which customers called person X?

#### The business value:

```
The person with phone number +86 921 547 9004 has been identified as a lead. We (company "Telecom") would like to know which of our customers have been in contact with this person since September 14th. This will help us in converting this lead into a customer.
```

#### As a statement:

```
Get me the customers of company “Telecom” who called the target person with phone number +86 921 547 9004 from September 14th onwards.
```

#### In Graql:

```graql
match
  $customer isa person has phone-number $phone-number;
  $company isa company has name "Telecom";
  (customer: $customer, provider: $company) isa contract;
  $target isa person has phone-number "+86 921 547 9004";
  (caller: $customer, callee: $target) isa call has started-at
  $started-at;
  $min-date == 2018-09-14T17:18:49; $started-at > $min-date;
get $phone-number;
```

#### The result:

```
[ '+62 107 530 7500', '+370 351 224 5176', '+54 398 559 0423',
  '+7 690 597 4443',  '+263 498 495 0617', '+63 815 962 6097',
  '+81 308 988 7153', '+81 746 154 2598']
```

## Try it yourself

{% include image.html file="examples-querying-kg-1.png" caption="Using the Visualiser" %}

> [**The Grakn visualiser**](http://dev.grakn.ai/docs/visualisation-dashboard/visualiser) provides a graphical tool to inspect and query your knowledge graph data.

{% include image.html file="examples-querying-kg-2.png" caption="Using the Graql Shell" %}

> [**The Graql Console**](ttp://dev.grakn.ai/docs/get-started/graql-console) is used to execute Graql queries from the command line, or to let Graql be invoked from other applications.

**Using a Driver**

<!-- tabs for client examples -->
<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active" style="width: 50% !important"><a href="#query-1-python" data-toggle="tab">Python</a></li>
    <li style="width: 50% !important"><a href="#query-1-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="query-1-python">
    {% gist 60ba56362e579e231519ad779240dba7 %}
  </div>
  <!-- with client nodejs -->
  <div role="tabpanel" class="tab-pane" id="query-1-nodejs">
    {% gist 8323495ece21277557f42b6a852a98f0 %}
  </div>
</div>

## Who are the people who have received a call from a London customer aged over 50 who has previously called someone aged under 20?

#### The business value:

```
We (company "Telecom") have received a number of harassment reports, which we suspect is caused by one individual. The only thing we know about the harasser is that he/she is aged roughly over 50 and lives in London. The reports have been made by young adults all aged under 20. We wonder if there is a pattern and so would like to speak to anyone who has received a call from a suspect, since he/she potentially started harassing.
```

#### As a statement:

```
Get me the phone number of people who have received a call from a customer aged over 50 after this customer (suspect) made a call to another customer aged under 20.
```

#### In Graql:

```graql
match
  $suspect isa person has city "London", has age > 50;
  $company isa company has name "Telecom";
  (customer: $suspect, provider: $company) isa contract;
  $pattern-callee isa person has age < 20;
  (caller: $suspect, callee: $pattern-callee) isa call has started at $pattern-call-date;
  $target isa person has phone-number $phone-number, has is-customer false;
  (caller: $suspect, callee: $target) isa call has started-at $target-call-date;
  $target-call-date > $pattern-call-date;
get $phone-number;
```

#### The result:

```
[ '+30 419 575 7546',  '+86 892 682 0628', '+1 254 875 4647',
  '+351 272 414 6570', '+33 614 339 0298', '+86 922 760 0418',
  '+86 825 153 5518',  '+48 894 777 5173', '+351 515 605 7915',
  '+63 808 497 1769',  '+27 117 258 4149', '+86 202 257 8619' ]
```

## Try it yourself

{% include image.html file="examples-querying-kg-3.png" caption="Using the Visualiser" %}
{% include image.html file="examples-querying-kg-4.png" caption="Using the Graql Shell" %}

**Using a Driver**

<!-- tabs for client examples -->
<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active" style="width: 50% !important"><a href="#query-2-python" data-toggle="tab">Python</a></li>
    <li style="width: 50% !important"><a href="#query-2-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="query-2-python">
    {% gist 970b237d467a48bbef447fcf9525c851 %}
  </div>
  <!-- with client nodejs -->
  <div role="tabpanel" class="tab-pane" id="query-2-nodejs">
    {% gist c381f2b6970987327a8a837d0e78b2c9 %}
  </div>
</div>

## Who are the common contacts of customers X and Y?

#### The business value:

```
The customer with phone number +7 171 898 0853 and +370 351 224 5176 have been identified as friends. We (company "Telecom") like to know who their common contacts are in order to offer them a group promotion.
```

#### As a statement:

```
Get me the phone number of people who have received calls from both customer with phone number +7 171 898 0853 and customer with phone number +370 351 224 5176.
```

#### In Graql:

```graql
match
  $common-contact isa person has phone-number $phone-number;
  $customer-a isa person has phone-number "+7 171 898 0853";
  $customer-b isa person has phone-number "+370 351 224 5176";
  (caller: $customer-a, callee: $common-contact) isa call;
  (caller: $customer-b, callee: $common-contact) isa call;
get $phone-number;
```

#### The result:

```
['+86 892 682 0628', '+54 398 559 0423']
```

## Try it yourself

{% include image.html file="examples-querying-kg-5.png" caption="Using the Visualiser" %}
{% include image.html file="examples-querying-kg-6.png" caption="Using the Graql Shell" %}

**Using a Driver**

<!-- tabs for client examples -->
<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active" style="width: 50% !important"><a href="#query-3-python" data-toggle="tab">Python</a></li>
    <li style="width: 50% !important"><a href="#query-3-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="query-3-python">
    {% gist 8a7e51657df4e098c0c9960dc698bc7c %}
  </div>
  <!-- with client nodejs -->
  <div role="tabpanel" class="tab-pane" id="query-3-nodejs">
    {% gist 6f09b8434985611ea79c14698d1ca464 %}
  </div>
</div>

## Who are the customers who 1) have all called each other and 2) have all called person X at least once?

#### The business value:

```
The person with phone number +48 894 777 5173 has been identified as a lead. We (company "Telecom") would like to know who his circle of  (customer) contacts are, so that we can encourage them in converting this lead to a customer.
```

#### As a statement:

```
Get me the phone phone number of all customers who have called each other as well the person with phone number +48 894 777 5173.
```

#### In Graql:

```graql
match
  $target isa person has phone-number "+48 894 777 5173";
  $company isa company has name "Telecom";
  $customer-a isa person has phone-number $phone-number-a;
  $customer-b isa person has phone-number $phone-number-b;
  (customer: $customer-a, provider: $company) isa contract;
  (customer: $customer-b, provider: $company) isa contract;
  (caller: $customer-a, callee: $customer-b) isa call;
  (caller: $customer-a, callee: $target) isa call;
  (caller: $customer-b, callee: $target) isa call;
get $phone-number-a, $phone-number-b;
```

#### The result:

```
[ '+62 107 530 7500', '+261 860 539 4754', '+81 308 988 7153' ]
```

## Try it yourself

{% include image.html file="examples-querying-kg-7.png" caption="Using the Visualiser" %}
{% include image.html file="examples-querying-kg-8.png" caption="Using the Graql Shell" %}

**Using a Driver**

<!-- tabs for client examples -->
<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active" style="width: 50% !important"><a href="#query-4-python" data-toggle="tab">Python</a></li>
    <li style="width: 50% !important"><a href="#query-4-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="query-4-python">
    {% gist eb72a49fc8b2ae7238f0d1b956628081 %}
  </div>
  <!-- with client nodejs -->
  <div role="tabpanel" class="tab-pane" id="query-4-nodejs">
    {% gist 8c7b5ad6b796f91e5603951ca8bb4937 %}
  </div>
</div>

## How does the average call duration among customers aged under 20 compare with those aged over 40?

#### The business value:

```
In order to better understand our customers' behaviour, we (company "Telecom") like to know how the average phone call duration among those aged under 20 compares to those aged over 40.
```

Two queries need to be executed to provide this insight.

### Query 1: aged under 20

#### As a statement:

```
Get me the average call duration among customers who have a contract with company "Telecom" and are aged under 20.
```

#### In Graql:

```graql
match
  $customer isa person has age < 20;
  $company isa company has name "Telecom";
  (customer: $customer, provider: $company) isa contract;
  (caller: $customer, callee: $anyone) isa call has duration
  $duration;
aggregate mean $duration;
```

#### The result:

```
1348 seconds
```

### Query 2: aged over 40

#### As a statement:

```
Get me the average call duration among customers who have a contract with company "Telecom" and are aged over 40.
```

#### In Graql:

```graql
match
  $customer isa person has age > 40;
  $company isa company has name "Telecom";
  (customer: $customer, provider: $company) isa contract;
  (caller: $customer, callee: $anyone) isa call has duration
  $duration;
aggregate mean $duration;
```

#### The result:

```
1587 seconds
```

## Try it yourself

{% include image.html file="examples-querying-kg-9.png" caption="Using the Graql Shell" %}

**Using a Driver**

<!-- tabs for client examples -->
<ul id="profileTabs" class="nav nav-tabs nav-tabs--intro">
    <li class="active" style="width: 50% !important"><a href="#query-5-python" data-toggle="tab">Python</a></li>
    <li style="width: 50% !important"><a href="#query-5-nodejs" data-toggle="tab">Node.js</a></li>
</ul>

<div class="tab-content tab-content--intro">
  <!-- with client python -->
  <div role="tabpanel" class="tab-pane active" id="query-5-python">
    {% gist 91ad4fa1f0f8860a706012d84f73b0d5 %}
  </div>
  <!-- with client nodejs -->
  <div role="tabpanel" class="tab-pane" id="query-5-nodejs">
    {% gist cc6c78bc3927b1e16696ba68976c9a4b %}
  </div>
</div>

## 👏 You’ve done it!

Five Graql queries, each written in a few lines, answered all of our questions.
Our imaginary client, Telecom, can now take these insights back to their team and, hopefully, use them responsibly to serve their customers.
And you ... are the one who made it happen!

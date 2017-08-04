---
title: Define a Hierarchical Ontology
keywords: overview
last_updated: February 2017
tags: [graql, java, graph-api]
summary: "How to build a hierarchical ontology"
sidebar: documentation_sidebar
permalink: /documentation/building-an-ontology/hierarchical-ontology.html
folder: documentation
comment_issue_id: 22
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

## Introduction

In this section we are going to expand the ontology we defined in the [Basic Ontology documentation](./basic-ontology.html), which we recommend you read before starting here. You may also find it helpful to refer to the [Knowledge Model](../the-fundamentals/grakn-knowledge-model.html) documentation.
We are going to introduce the idea of making ontologies deeper and more meaningful by defining a hierarchy of types.

When we left off, our ontology looked as follows:

![Ontology](/images/basic-ontology1.png)

```graql
insert
 
# Entities
    
  person sub entity
    has identifier
    has firstname
    has surname
    has middlename
    has picture
    has age
    has birth-date
    has death-date
    has gender
    plays parent
    plays child
    plays spouse1
    plays spouse2;

# Resources

  identifier sub resource datatype string;
  name sub resource datatype string;
  firstname sub name datatype string;
  surname sub name datatype string;
  middlename sub name datatype string;
  picture sub resource datatype string;
  age sub resource datatype long;
  "date" sub resource datatype string;
  birth-date sub "date" datatype string;
  death-date sub "date" datatype string;
  gender sub resource datatype string;

# Roles and Relations

  marriage sub relation
    relates spouse1
    relates spouse2
    has picture;

  spouse1 sub role;
  spouse2 sub role;

  parentship sub relation
    relates parent
    relates child;

  parent sub role;
  child sub role;
```
    
This ontology represents a genealogy graph which models a family tree.
This is a very simplistic ontology with plenty of room for extension, so let's begin!
 
## Hierarchies of Entity Types

It is possible to define entity types more granularly. Think of sub-categories that enable additional details to be embedded in the ontology. 
For example, if we have a entity type called `vehicle`, we can break that down further by differentiating between `cars` and `motorbikes`. This can be done as follows:

```graql
insert

vehicle sub entity;
car sub vehicle;
motorbikes sub vehicle;
```    
    
In the above example we are saying that a `car` is a subtype (a specialised type) of a `vehicle`. This means that when adding data to our graph, when we know we have a `vehicle`, we can also differentiate between a `car` and a `motorbike`.
    
So how can we use this technique to improve our existing genealogy ontology?
  
We could specialise the `person` entity into `man` and `woman` for example. However, for the sake of making things more interesting, we are going to introduce a new entity to the graph. A family is made up not only of people but of events, like births, weddings, funerals, and many others, which link those people together and better define their lives. 

We can model this as follows:

```graql    
event sub entity
  is-abstract
  has degree
  has confidence
  has "date"
  has identifier
  has notes
  plays conclusion
  plays happening;
    
wedding sub event;
 
funeral sub event
  has death-date;
    	
christening sub event
  has birth-date;
    
birth sub event
  has firstname
  has middlename
  has surname
  has gender
  has birth-date;
    	
death sub event
  has death-date;
``` 	      
  	    
Notice that for the `event` entity type we added `is-abstract`, this is an optional additional restriction to ensure that we do not create any instances of `event`, but instead use the most granular definitions provided, i.e. `birth`, `death`, etc . . .  

## Hierarchies of Relation Types and Role Types

Grakn also allows you to design hierarchies of relation types and role types, enabling the ontology to be deeper and more expressive. For example, if we have a relation type called `partnership` between two people we can expand on this by defining more detailed partnerships; `civil-partnership`, `marriage`, `unions`, etc.

Now lets take a look at expanding our genealogy ontology. When modelling a domain there are many ways of doing so. For this example we are going to redo the `marriage` relation type so that it can provide more meaning:

```graql
insert

relatives sub relation
  is-abstract;

marriage sub relatives
  relates spouse1
  relates spouse2
  relates husband
  relates wife
  has "date";
	    
spouse sub role;
spouse1 sub spouse;
spouse2 sub spouse;
husband sub spouse;
wife sub spouse;
```
    
	    
We have defined a new super type called `relatives` which enables us to link generic relatives together, and we have said that marriage is a type of relative relation. We have also expanded on the roles which make up a marriage, enabling us to be more expressive and detailed about the domain we are modelling.
From now on, we can be clear if a person is a `husband` or a `wife` or just a `spouse` in a marriage. Note that, when we query for people who play the role of a `spouse` we will get all the `husbands` and `wives` as well.
 
 
Lets expand this even further:

```graql
insert

parentship sub relatives
  relates parent
  relates mother
  relates father
  relates child
  relates son
  relates daughter;
    
parent sub role;
mother sub parent;
father sub parent;
child sub role;
son sub child;
daughter sub child;
```

Now we have provided more detail about being a parent. 
We have also said that being a parent is a `relatives` relation. 
This is quite useful because when we ask for all relatives we will be getting relatives via birth and via marriage.
 
## Wrapping up 

We could go into far more detail regarding our genealogy graph but I will leave that to you.
For the moment here is our more complex ontology to get you started on making your own deeper ontologies. You can find the ontology, the dataset and rules that accompany it, on Github in our [sample-datasets repository](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph):

```graql
insert

# Entities

  person sub entity
    has gender
    has birth-date
    has death-date
    has identifier
    has firstname
    has middlename
    has surname
    plays spouse
    plays parent
    plays child
    plays sibling;
        
    gender sub resource datatype string;
    birth-date sub "date";
    death-date sub "date";
    name sub resource datatype string;
    firstname sub name;
    middlename sub name;
    surname sub name;
    identifier sub resource datatype string;
    
  event sub entity
    is-abstract
    has degree
    has confidence
    has "date"
    has identifier
    has notes
    plays conclusion
    plays happening;
    
  wedding sub event;
    
  funeral sub event
    has death-date;
        
  christening sub event
    has birth-date;
    
  birth sub event
    has firstname
    has middlename
    has surname
    has gender
    has birth-date;
        
  death sub event
    has death-date;   	    
    
## Relations
    
  relatives sub relation
    is-abstract;
    
  marriage sub relatives
    relates spouse1
    relates spouse2
    relates husband
    relates wife
    has "date";
    
  spouse sub role;
  spouse1 sub spouse;
  spouse2 sub spouse;
  husband sub spouse;   
  wife sub spouse;
    
  parentship sub relatives
    relates parent
    relates mother
    relates father
    relates child
    relates son
    relates daughter;
  
  parent sub role;
  mother sub parent;
  father sub parent;
  child sub role;
  son sub child;
  daughter sub child;
```

{% include links.html %}

## Where Next?

We will continue to explore the development of an ontology in the next section on defining a [rule-driven ontology](./rule-driven-ontology.html).

You can find the complete ontology for our genealogy graph project, the dataset and rules that accompany it, on Github in our [sample-datasets repository](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph).

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/22" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.
has
---
title: Building the schema (Continued)
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will complete your first Grakn schema, adding roles to what you have built in the last lesson.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/schema-building-continued.html
folder: overview
toc: false
KB: academy
---

## The story so far

In the [last lesson](./schema-building.html) you have started building the schema that models the example problem for the academy dataset (if you need to review the problem, [here is the link](./graql-intro.html)). If you have followed all the exercises, you should by now have a file called `schema.gql` that looks more or less like this:

```graql
define
"bond" sub entity;
"company" sub entity;
"oil-platform" sub entity has distance-from-coast;
"article" sub entity has subject;
"country" sub entity has name;

"owns" sub relationship;
"issues" sub relationship;
"located-in" sub relationship;

"name" sub attribute datatype string;
"subject" sub attribute datatype string;
"distance-from-coast" sub attribute datatype long;
```

So far, so good. The types that you have added to the file currently "live on their own"; to link them together, we need to add one last concept to the schema: roles. Roles help to define how things are related and what the (wait for it) role of each entity in this relation is. They also help to guarante logical integrity (i.e. the rational correctness of your data) avoiding, for example, to have cats married to buildings in your data, unless you explicitly allow that in your schema.

There are three steps in defining roles. Let’s examine them one by one.

## Defining relationships with __relates__
The first step is to look at our relationships one by one and think about what they need to link together. Take `owns`, for example: in our domain, it is used to indicate the connection between an oil-platform and the company it belongs to. There are thus two roles in the "owns" relationship (but there could be also only one or more than two): we will call them `owner` and `owned` (because the author and has no fantasy :) ). In order to specify the relation in this way, we will use the keyword `relate` and the definition of "owns" will then look like this:


```graql-skip-test
"owns" sub relationship relates owner relates owns;
```

## Defining roles
The second step is super easy: the role concepts related to our relationships need to be defined in our schema file. That works exactly like defining types, so you should be able to guess how to do it (a little hint: just as an entity is a sub of `entity` a new role should be a sub of `role`).


## Who does what?
The final step is to define which concepts are allowed to play which roles. To do this, we use the keyword `plays`. Every role must be played by at least one concept type, but there is no upper limit to it. To decide which type can play which role refer back to the conceptual model. For example, we know that we want companies to play the role of owner and oil platforms to play the role of owned. The type definitions will then look like this:

```graql-skip-test
"company" sub entity plays owner;
"oil-platform" sub entity has distance-from-coast plays owned;
```

You should know by now, that there is no specific reason to put the whole type definition in one single line, so the definition above could also be rewritten as

```graql-skip-test
"company" sub entity
   plays owner;
"oil-platform" sub entity
   has distance-from-coast
   plays owned;
```

It really is a matter of taste.

You are almost there: go ahead and add the relevant roles for the remaining relationships.


### Achievement Unlocked!
Your schema file should at this point look more or less like the following:


```graql-skip-test
define
"bond" sub entity
    plays issued;
"company" sub entity
    plays owner
    plays issuer;
"oil-platform" sub entity
    has distance-from-coast
    plays owned
    plays located;
"article" sub entity
    has subject;
"country" sub entity
    has name
    plays location;

"owns" sub relationship
    relates owner
    relates owned;
"issues" sub relationship
    relates issuer
    relates issued;
"located-in" sub relationship
    relates location
    relates located;
"owner" sub role; "owned" sub role;
"location" sub role; "located" sub role;
"issued" sub role; "issuer" sub role;

"name" sub attribute datatype string;
"subject" sub attribute datatype string;
"distance-from-coast" sub attribute datatype long;
```

Congratulations! You have built your first working Grakn schema! Of course, this is just a starting point and when you start putting data into your knowledge graph you will realise that you need to extend the schema (for example you might want to allow companies to have names), but the one you have built is valid and working and could be loaded into Grakn as is (you will learn how in the next module of the Academy). There is one more topic you need to learn about before heading to the module review. Let's have a look at how we can modify an existing Grakn schema.


## Changing the schema
One of the best features of Grakn schemas is the fact that they are flexible. At any point in time you can add types, attributes to types, roles and so on.

But what about deleting a type? Well, that is a bit more delicate. Imagine you have stored information about a thousand articles in your knowledge graph and suddenly you delete the type `article`. What would happen to all those articles? You wouldn't want to destroy them, would you? For this reason, in order to remove a type you first have to delete all of its instances.

Once you have done that, you are free to modify your schema as you see fit. In order to undo something that is defined in your schema, you need to use the keyword `undefine`. Do you want to remove the type `article`?

```graql
undefine article sub entity;
```

Do you want to remove the connection between a country and its name (because for some reason you still want countries but you don't want them to have names)?

```graql
undefine country has name;
```

And so on and so forth. Notice that in the first example, you do not simply `undefine article`, because you need to specify _what_ about the article you want to undefine. Notice as well that in the second example the query is only affecting the connection between `country` and `name`, but it's not deleting either type. Basically `undefine` is the exact opposite of `define` (as one would expect): if you can create it with `define`, you can remove it with `undefine`.


## What next?
You can now proceed to the [next lesson](./schema-review.html) to review the schema building process and to check that you remember what you have learned so far about Grakn schemas. After that it will be time to load data. Our knowledge graph will start to look more and more like the one you have seen at the beginning of the Academy when you were still learning about the Graql basics.

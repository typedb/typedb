---
title: Migrating JSON data to Grakn
keywords: setup, getting started
tags: [migration]
summary: "This document will teach you how to migrate JSON data into Grakn."
sidebar: documentation_sidebar
permalink: /docs/migrating-data/migrating-json
folder: docs
KB: pokemon
---

## Introduction
This tutorial shows you how to populate Grakn with JSON data. If you have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide).

## Migration Shell Script for JSON
The migration shell script can be found in */bin* directory of your Grakn environment. We will illustrate its usage in an example below:

```bash
usage: graql migrate json --template <arg> --input <arg> --keyspace <arg> [--help] [--no] [--batch <arg>] [--active <arg>] [--uri <arg>] [--retry <arg>] [--verbose]

OPTIONS
 -a,--active <arg>     Number of tasks (batches) running on the server at
                       any one time. Default 25.
 -b,--batch <arg>      Number of rows to execute in one Grakn transaction.
                       Default 25.
 -c,--config <arg>     Configuration file.
 -h,--help             Print usage message.
 -i,--input <arg>      Input json data file or directory.
 -k,--keyspace <arg>   Grakn knowledge base. Required.
 -n,--no               Write to standard out.
 -r,--retry <arg>      Retry sending tasks if engine is not available
 -t,--template <arg>   Graql template to apply to the data.
 -u,--uri <arg>        Location of Grakn Engine.
 -v,--verbose          Print counts of migrated data.
 -d,--debug            Migration immediatly stops if any transaction fails
```

{% include note.html content="The JSON migrator can handle either a directory or a file as the -input parameter!" %}

## JSON Migration Basics

The steps to migrate the CSV to GRAKN.AI are:

* define a schema for the data to derive the full benefit of a knowledge base
* create templated Graql to map the data to the schema. Approach each JSON file as though you were inserting a single query, taking care that there are not more than one `match` or `insert` commands in your template.
* invoke the Grakn migrator through the shell script or Java API.

{% include note.html content="JSON Migration makes heavy use of the Graql templating language. You will need a foundation in Graql templating before continuing, so please read through our [migration langauge documentatino](../migrating-data/migration-language) to find out more." %}

### Looping over a JSON array   

As an example, let's take some JSON:

<!-- TODO: Change this from pokemon examples -->
```json
{
    "types": [
    {"id":"1", "type": "normal" },
    {"id":"2", "type": "fighting" },
    {"id":"3", "type": "flying" },
    {"id":"4", "type": "poison" },
    {"id":"5", "type": "ground" },
    {"id":"6", "type": "rock" },
    {"id":"7", "type": "bug" },
    {"id":"8", "type": "ghost" },
    {"id":"9", "type": "steel" },
    {"id":"10", "type": "fire" }]
}
```

To migrate all of these types, we need to iterate over the array:    

```graql-template
insert
for(<types>) do {
    $x isa pokemon-type
        has description <type>
        has type-id <id>;
}
```

Which will resolve as:    

```graql
insert $x0 has type-id "1" has description "normal" isa pokemon-type;
$x1 has description "fighting" has type-id "2" isa pokemon-type;
$x2 has type-id "3" has description "flying" isa pokemon-type;
$x3 has type-id "4" has description "poison" isa pokemon-type;
$x4 has description "ground" has type-id "5" isa pokemon-type;
$x5 has description "rock" has type-id "6" isa pokemon-type;
$x6 has description "bug" has type-id "7" isa pokemon-type;
$x7 has description "ghost" has type-id "8" isa pokemon-type;
$x8 has type-id "9" has description "steel" isa pokemon-type;
$x9 has description "fire" isa pokemon-type has type-id "10";
```

### Match-Inserts with loops   

In some situations, you'll need to look up references to existing entities so that you can refer to them when inserting data.

Once the above types are migrated, we can move on to the pokemon JSON file:   

```json
{
    "pokemon": [
        {
            "identifier":"Charmander",
            "id":4,
            "species_id":4,
            "height":6,
            "weight":85,
            "types":[
                "1"
            ]
        },{
            "identifier":"Charmeleon",
            "id":"5",
            "species_id":5,
            "height":11,
            "weight":190,
            "types":[
                "1"
            ]
        },{
            "identifier":"Charizard",
            "id":"6",
            "species_id":6,
            "height":17,
            "weight":905,
            "types":[
                "1", "2"
            ]
        }
    ]
}
```

This template is rather complicated. The first `match` portion is necessary to look up all of the already migrated pokemon types in the knowledge base. You can then refer to these variables in your `insert` statement while creating the relationships.   

```graql-template
match
   for(<pokemon>) do {
        for(t in <types>) do {
            $<t> has type-id <t>;
        }
   }

insert
for(<pokemon>) do {
    $p isa pokemon
        has weight <weight>
        has height <height>
        has pokedex-no <id>
        has description <identifier>;

    for(t in <types>) do {
        (pokemon-with-type: $p, type-of-pokemon: $<t>) isa has-type;
    }
}
```

The resulting Graql statement, if printed out, looks as follows:

```graql
match $2 has type-id "2"; $1 has type-id "1";
insert $p0 isa pokemon has weight 85 has height 6 has pokedex-no 4 has description "Charmander";
(pokemon-with-type: $p0, type-of-pokemon: $1) isa has-type;
$p1 isa pokemon has weight 190 has height 11 has pokedex-no 5 has description "Charmeleon";
(pokemon-with-type: $p1, type-of-pokemon: $1) isa has-type;
$p2 isa pokemon has weight 905 has height 17 has pokedex-no 6 has description "Charizard";
(pokemon-with-type: $p2, type-of-pokemon: $1) isa has-type;
(pokemon-with-type: $p2, type-of-pokemon: $2) isa has-type;
```

## Where Next?
You can find further documentation about migration in our API reference documentation (which is in the */docs* directory of the distribution zip file, and also online [here](http://javadoc.io/doc/ai.grakn/grakn). An example of JSON migration using the Java API can be found on [Github](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-giphy).

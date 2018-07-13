---
title: Using Grakn for Statistical Analysis
keywords: analytics, machine-learning
tags: [analytics, examples]
summary: "How to use Grakn for statistical analysis"
sidebar: documentation_sidebar
permalink: /docs/examples/analytics
folder: docs
---


## Introduction

This example illustrates the use of `aggregate` and `compute` queries in Graql using a simple dataset to calculate statistics. The code for the example can be found on our [sample-projects](https://github.com/graknlabs/sample-projects/tree/master/example-analytics-mtcars) repository, and is also included in the *examples* folder of the Grakn distribution from v0.12.0.

For a detailed overview of calculating statistics using Graql, we recommend that you take a look at the documentation for:

* [aggregate queries](../api-references/dml#aggregate)
* [compute queries](../distributed-analytics/compute-queries)

## Data

This example takes a dataset that will be familiar to students of R - [mtcars (Motor Trend Car Road Tests) data](https://stat.ethz.ch/R-manual/R-devel/library/datasets/html/mtcars.html).  The data was extracted from the 1974 Motor Trend US magazine, and comprises fuel consumption and 10 other aspects of automobile design and performance for 32 automobiles (1973–74 models). We have created a csv file of the data and added two columns to indicate the car maker's name and region that the car was made in (Europe, Japan or North America). The readme file in the repository gives further information for anyone who wishes to migrate the mtcars data directly, but for the purposes of this example we provide a [single data file that you can load](#data-migration) to populate a graph.

## Schema

We have provided the following schema to represent the data, although many other variations are possible:

```graql
define

# Entities

vehicle sub entity
    is-abstract;

car sub vehicle
    is-abstract

    has model
    has mpg
    has cyl
    has disp
    has hp
    has wt
    has gear
    has carb
    plays made;

automatic-car sub car;
manual-car sub car;

carmaker sub entity
    is-abstract
    has maker-name
    plays maker;

japanese-maker sub carmaker;
american-maker sub carmaker;
european-maker sub carmaker;

# Resources

model sub attribute datatype string;
maker-name sub attribute datatype string;
mpg sub attribute datatype double;
cyl sub attribute datatype long;
disp sub attribute datatype double;
hp sub attribute datatype long;
wt sub attribute datatype double;
gear sub attribute datatype long;
carb sub attribute datatype long;
powerful sub attribute datatype string;
economical sub attribute datatype string;

# Roles and Relations

manufactured sub relationship
    relates maker
    relates made;
```

To load *schema.gql* into Grakn, make sure the engine is running and choose a clean keyspace in which to work (here we use the default keyspace, so we are cleaning it before we get started).

```bash
./grakn server clean
./grakn server start
./graql console -f ./schema.gql
```		

## Data Migration

We migrated the CSV data using template Graql files, but for ease of use, we provide a single data file that you can load to populate a knowledge graph.

```bash
./graql console -b ./data.gql
```		

Spin up the [Grakn visualiser](../visualisation-dashboard/visualiser) by pointing your browser to [http://localhost:4567/](http://localhost:4567/). You can submit queries to check the data, or explore it using the Types dropdown menu.

Some sample queries:

```graql
# Cars where the model name contains "Merc"
match $x has model contains "Merc"; get;

# Cars with more than 4 gears
match $x has gear > 4; get;

# Japanese-made cars that are manual
match $x isa manual-car has model $s; $y isa japanese-maker; (made: $x, maker:$y); get;

# European cars that are automatic
match $x isa automatic-car has model $s; $y isa european-maker; (made: $x, maker:$y); get;

```

At this point, you are ready to start investigating statistics within the data using Graql
[`aggregate`](../api-references/dml#aggregate) and [`compute`](../distributed-analytics/compute-queries) queries:

## `aggregate`

You cannot make [`aggregate`](../api-references/dml#aggregate) queries from within the **graph** view in the Grakn
visualiser, so you will need to switch views using the left hand navigation pane, from **Graph** to **Console**. This
shows a read-write view on Grakn, and you can now submit queries in the usual way, via the form. Alternatively, from
your terminal, you can start the Graql shell in its interactive (REPL) mode by typing `./graql console` at the terminal,
from within the *bin* directory of the Grakn installation.

Here are some example `aggregate` queries to try:

```graql
# Count of all cars
match $x isa car; aggregate count; # 32

# Count American car makers
match $x isa american-maker; aggregate count; # 6

# Maximum MPG for an automatic car
match $x isa automatic-car, has mpg $a; aggregate max $a; # 24.4

# Minimum HP for all cars
match $x isa car, has hp $hp; aggregate min $hp; # 52

# Mean MPG for manual and automatic cars
match $x isa manual-car has mpg $mpg; aggregate mean $mpg; # 24.39
match $x isa automatic-car has mpg $mpg; aggregate mean $mpg; # 17.15

# Median number of cylinders (all Mercedes cars)
match $x isa carmaker has maker-name contains "Mercedes"; $y isa car has cyl $c; (maker:$x, made:$y); aggregate median $c; # 6

# Or...
match $x has model contains "Merc", has cyl $c; aggregate median $c; # 6

# Maximum number of carburetors (all Chrysler cars)
match $x isa carmaker has maker-name contains "Chrysler"; $y isa car has carb $c; (maker:$x, made:$y); aggregate max $c; # 4

# Minimum number of gears (all cars)
match $x isa car, has gear $g; aggregate min $g; # 3
```


## `compute`

Graql also provides [compute queries](../distributed-analytics/compute-queries) that can be used to determine values such as mean, minimum and maximum. These can be submitted using the **graph** view on the Visualiser. For example, type each of the following into the form and submit:

```graql
# Number of automatic and manual cars
compute count in automatic-car; # 19
compute count in manual-car; # 13

# Number of Japanese car makers
compute count in japanese-maker; # 4

# Median number of cylinders (all cars)
compute median of cyl;  # 6

# Minimum number of gears (all cars)
compute min of gear; # 3

# Maximum number of carburetors (all cars)
compute max of carb; # 8

# Mean MPG for an automatic car
compute mean of mpg, in automatic-car; # 17.15

# Mean MPG for a manual car
compute mean of mpg, in manual-car; # 24.39

# Median number of cylinders (all Japanese cars)


# Maximum number of carburetors (all American cars)


```


## When to Use `aggregate` and When to Use `compute`?

Aggregate queries are computationally light and run single-threaded on a single machine, and are more flexible than the equivalent compute query (for example, you can use an aggregate query to filter results by attribute).

```graql
match $x isa car has model contains "Merc"; aggregate count; # 7
```

Compute queries are computationally intensive and run in parallel on a cluster, so are good for big data and can be used to calculate results very fast. However, you can't filter the results by attribute in the same way as you can for an `aggregate` query.


## Inference

```graql
>>> match $x has model $s, has powerful "TRUE" has economical "TRUE";
$x id "106584" isa manual-car; $y val "Ferrari Dino" isa model;
$x id "254120" isa automatic-car; $y val "Pontiac Firebird" isa model;
```

## Where Next?

If you haven't already, we recommend that you review the documentation about [Graql analytics](../distributed-analytics/overview), since there is more to `compute` than just statistical analysis. Unfortunately, this example is not a good one to illustrate clusters, degrees or shortest path analytics, which is why it isn't described here. There is also an example of using Graql analytics on the genealogy dataset available [here](../examples/java-analytics.html).

This example was based on CSV data migrated into Grakn. Having read it, Yyou may want to further study our documentation about [CSV migration](../migrating-data/migrating-csv) and [Graql migration language](../migrating-data/migration-language).  

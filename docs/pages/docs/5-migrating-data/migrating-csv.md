---
title: Migrating CSV data to Grakn
keywords: setup, getting started
tags: [migration]
summary: "This document will teach you how to migrate CSV data into Grakn."
sidebar: documentation_sidebar
permalink: /docs/migrating-data/migrating-csv
folder: docs
---

## Introduction
This tutorial shows you how to populate Grakn with CSV data. If you have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide).

## Migration Shell Script for CSV
The migration shell script can be found in */bin* directory of your Grakn environment. We will illustrate its usage in an example below:

```bash
usage: ./graql migrate csv -template <arg> -input <arg> -keyspace <arg> [-help] [-no] [-separator <arg>] [-active <arg>] [-batch <arg>] [-uri <arg>] [-null <arg>] [-quote <arg>] [-r <arg>] [-v]

OPTIONS
 -a,--active <arg>      Number of tasks (batches) running on the server at
                        any one time. Default 25.
 -b,--batch <arg>       Number of rows to execute in one Grakn
                        transaction. Default 25.
 -c,--config <arg>      Configuration file.
 -h,--help              Print usage message.
 -i,--input <arg>       Input csv file.
 -k,--keyspace <arg>    Grakn knowledge base. Required.
 -l,--null <arg>        String that will be evaluated as null.
 -n,--no                Write to standard out.
 -q,--quote <arg>       Character used to encapsulate values containing
                        special characters.
 -r,--retry <arg>       Retry sending tasks if engine is not available
 -s,--separator <arg>   Separator of columns in input file.
 -t,--template <arg>    Graql template to apply to the data.
 -u,--uri <arg>         Location of Grakn Engine.
 -v,--verbose           Print counts of migrated data.
 -d,--debug            Migration immediatly stops if any transaction fails
```

## CSV Migration Basics

The steps to migrate the CSV to GRAKN.AI are:

* define a schema for the data to derive the full benefit of a knowledge base
* create templated Graql to map the data to the schema
* invoke the Grakn migrator through the shell script or Java API. The CSV migrator will apply the template to each row of data in the CSV file, replacing the sections indicated in the template with provided data: the column header is the key and the content of each row at that column the value.

{% include note.html content="CSV Migration makes heavy use of the Graql templating language. You will need a foundation in Graql templating before continuing, so please read through our [migration langauge documentatino](../migrating-data/migration-language) to find out more." %}


## Example: Cars

Let's take a simple example. First, the CSV file, *cars.csv*:

```csv
Year,Make,Model,Description,Price
1997,Ford,E350,"ac, abs, moon",3000.00
1999,Chevy,"Venture",,4900.00
1996,Jeep,Grand Cherokee,"MUST SELL! air, moon roof, loaded",4799.00
```

Here is the schema for the example:   

```graql
define

car sub entity
  has name
  has year
  has description
  has price;

name sub attribute datatype string;
year sub attribute datatype string;
description sub attribute datatype string;
price sub attribute datatype double;

```

Make sure to load your schema into the knowledge base:

```bash
./graql console -f ./schema.gql -k grakn
```

And the Graql template, *car-migrator.gql*:   

```graql-template
insert                                                                                                                             

$x isa car
  has name @concat(<Make>, "-", <Model>)
  has year <Year>
  has price @double(<Price>)
  if (<Description> != "") do { has description <Description>};  
```

The template will create a `car` entity for each row. It will attach `year` and `price` resources to each of these entities. If the `description` attribute is present in the data, it will attach the appropriate `description` to the `car`.

The template is applied to each row by calling the migration script:

```bash
./graql migrate csv -i ./cars.csv -t ./car-migrator.gql -k grakn
```

The resulting Graql statement, if printed out, looks as follows:

```graql
insert $x0 isa car has name "Ford-E350" has year "1997" has price 3000.0 has description "ac, abs, moon";
insert $x0 isa car has name "Chevy-Venture" has year "1999" has price 4900.0;
insert $x0 isa car has name "Jeep-Grand Cherokee" has year "1996" has price 4799.0 has description "MUST SELL! air, moon roof, loaded";
```

You will note that the second Graql insert is missing the `description` attribute. This is because that value is not present in the data and the template uses an `if` statement to check if it exists.

### Separator

The `separator` option allows you to specify the column separator. With this we are able to migrate a wider range of formats, including TSV.

```tsv
Year  Make  Model Description Price
1997  Ford  E350  "ac  abs   moon"  3000.00
1999  Chevy "Venture" ""  4900.00
1996  Jeep  Grand Cherokee  "MUST SELL!
air  moon roof   loaded"  4799.00
```

This file would be migrated in the same way as the previous example when you specify the separator using the `-s \t` argument:

```bash
./graql migrate csv -i ./cars.tsv -t ./car-migrator.gql -s \t -k grakn
```

## Where Next?
We have an additional, more extensive, example that [migrates genealogy data from CSV](../examples/CSV-migration). Our [sample-projects repository on Github](https://github.com/graknlabs/sample-projects) also contains [an example that migrates a simple CSV pets dataset](https://github.com/graknlabs/sample-projects/tree/master/example-csv-migration-pets), and another [example for video games](https://github.com/graknlabs/sample-projects/tree/master/example-csv-migration-games), was described in a separate [blog post](https://blog.grakn.ai/twenty-years-of-games-in-grakn-14faa974b16e#.do8tq0dm8).

You can find further documentation about migration in our API reference documentation (which is in the */docs* directory of the distribution zip file, and also online [here](http://javadoc.io/doc/ai.grakn/grakn).

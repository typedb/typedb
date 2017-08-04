---
title: An Example of Migrating CSV to Grakn
keywords: migration
last_updated: January 2017
tags: [migration, examples]
summary: "An example to illustrate migration of CSV to Grakn"
sidebar: documentation_sidebar
permalink: /documentation/examples/CSV-migration.html
folder: documentation
comment_issue_id: 27
---


## Introduction

This example looks at the migration of genealogy data in CSV format to build a graph in GRAKN.AI. The data is used as the basis of a [blog post](https://blog.grakn.ai/family-matters-1bb639396a24#.2e6h72y0m) that illustrates the fundamentals of the Grakn visualiser, reasoner and analytics components. 

As the blog post explains, the original data was a [document](http://www.lenzenresearch.com/titusnarrlineage.pdf) from [Lenzen Research](http://www.lenzenresearch.com/) that described the family history of Catherine Niesz Titus for three generations of her maternal lineage.

In this example, we will walk through how to migrate the CSV data into Grakn, and confirm that we have succeeded using the Grakn visualiser. 

For a detailed overview of CSV migration, we recommend that you take a look at the Grakn documentation on [CSV Migration](https://grakn.ai/pages/documentation/migration/CSV-migration.html) and [Graql templating](https://grakn.ai/pages/documentation/graql/graql-templating.html).  

## Genealogy Data

The data for this example can be found as a set of CSV files in the [sample-projects](https://github.com/graknlabs/sample-projects/tree/master/example-csv-migration-genealogy) repository on Github, which is also included in the Grakn distribution zip file. The data was put together by our team from narrative information gleaned from the original Lenzen Research [document](http://www.lenzenresearch.com/titusnarrlineage.pdf), with some minor additions to generate some interesting queries for Grakn's reasoner.

Let's take a look at the *raw-data* directory in the example project, which contains the CSV files. These files were put together by hand by our team, mostly by [Michelangelo](https://blog.grakn.ai/@doctormiko).

|filename|description|
|[*people.csv*](https://github.com/graknlabs/sample-projects/blob/master/example-csv-migration-genealogy/raw-data/people.csv)| This comprehensive CSV contains information about all the individuals discussed in the Lenzen document. For each row, it lists the available information about individuals' names, gender, birth and death dates and age at death. It also assigns each person a person ID ("pid"), which is a string containing the full name of each individual, and is used for identification of individuals in the other CSV files.|
|[*births.csv*](https://github.com/graknlabs/sample-projects/blob/master/example-csv-migration-genealogy/raw-data/births.csv)| This CSV lists the person IDs of a child and each of its parents|
|[*weddings.csv*](https://github.com/graknlabs/sample-projects/blob/master/example-csv-migration-genealogy/raw-data/weddings.csv)| This CSV comprises a row for each marriage, identifying each by a wedding ID (wid). The rows contain the person IDs of each spouse and the date of their wedding, where it is known.|

### Ontology

The ontology is a way to describe the entities and their relationships, so the underlying graph can store them as nodes and edges. You can find out more in our guide to the Grakn Knowledge Model. The ontology allows Grakn to perform:

* logical reasoning over the represented knowledge, such as the extraction of implicit information from explicit data (inference)
* discovery of inconsistencies in the data (validation).

The ontology is shown below. There is a single entity, `person`, which has a number of resources and can play various roles (`parent`, `child`, `spouse1` and `spouse2`) in two possible relations (`parentship` and `marriage`).

```graql
insert

# Entities

person sub entity
  plays parent
  plays child
  plays spouse1
  plays spouse2

  has identifier
  has firstname
  has surname
  has middlename
  has picture
  has age
  has birth-date
  has death-date
  has gender;

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
``` 

To load *ontology.gql* into Grakn, make sure the engine is running and choose a clean keyspace in which to work (here we use the default keyspace, so we are cleaning it before we get started). 

```bash
<relative-path-to-Grakn>/bin/grakn.sh clean
<relative-path-to-Grakn>/bin/grakn.sh start
<relative-path-to-Grakn>/bin/graql.sh -f ./ontology.gql
```
		

### Data Migration

Having loaded the ontology, the next steps are to populate the graph by migrating data into Grakn from CSV. 

We will consider three CSV files that contain data to migrate into Grakn. 

#### people.csv

The *people.csv* file contains details of the people that we will use to create seven `person` entities. Note that not all fields are available for each person, but at the very least, each row is expected to have the following:

* pid (this is the person identifier, and is a string representing their full name)
* first name
* gender

```csv
name1,name2,surname,gender,born,dead,pid,age,picture
Timothy,,Titus, male,,,	Timothy Titus,,	
Mary,,Guthrie,female,,,Mary Guthrie,,	
John,,Niesz,male,1798-01-02,1872-03-06,John Niesz,74,
Mary,,Young,female,1798-04-09,1868-10-28,Mary Young,70,
William,Sanford,Titus,male,1818-03-23,01/01/1905,William Sanford Titus,76,
Elizabeth,,Niesz,female,1820-08-27,1891-12-08,Elizabeth Niesz,71,
Mary,Melissa,Titus,female,1847-08-12,10/05/1946,Mary Melissa Titus,98,
...
```

The migrator is a set of template Graql statements that instruct the Grakn migrator on how the CSV data can be mapped to the ontology. The Grakn migrator applies the template to each row of data in a CSV file, replacing the indicated sections in the template with the value from a specific cell, identified by the column header (the key). This sounds complicated, but isn't really, as we will show.

The Graql template code for the people migrator is as follows:

```graql-template
insert
  $p isa person has identifier <pid>
  has firstname <name1>,
		
  if (<surname> != "") do 
    {
    has surname <surname>,
    }

  if (<name2> != "") do 
    {
    has middlename <name2>,
    }

  if (<picture> != "") do 
    {
    has picture <picture>,
    }

  if (<age> != "") do 
    {
    has age @long(<age>),
    }

  if (<born> != "") do 
    {
    has birth-date <born>,
    }

  if (<dead> != "") do 
    {
    has death-date <dead>,
    }

  has gender <gender>;
```

For each row in the CSV file, the template inserts a `person` entity with resources that take the value of the cells in that row. Where data is optional, the template checks to see if it is present before adding the resources for middlename, surname, picture, age, birth and death dates. 

Calling the Grakn migrator on the *people.csv* file using the above template (named `people-migrator.gql`) is performed as follows:

```bash
<relative-path-to-Grakn>/bin/migration.sh csv -i ./people.csv -t ./migrators/people-migrator.gql -k grakn
```

{% include note.html content="Usage instructions for the Grakn migrator are available as part of our [migration documentation](../migration/migration-overview.html)." %}

The data insertion generated by the migrator is as follows:

```graql
insert $p0 has death-date "1891-12-08" isa person has gender "female" has identifier "Elizabeth Niesz" has surname "Niesz" has age 71 has firstname "Elizabeth" has birth-date "1820-08-27";
insert $p0 has identifier "William Sanford Titus" has age 76 isa person has firstname "William" has surname "Titus" has gender "male" has birth-date "1818-03-23" has middlename "Sanford" has death-date "1905-01-01";
insert $p0 isa person has surname "Titus" has firstname "Timothy" has identifier "Timothy Titus" has gender "male";
insert $p0 isa person has firstname "Mary" has identifier "Mary Guthrie" has surname "Guthrie" has gender "female";
insert $p0 isa person has firstname "Mary" has death-date "1946-05-10" has surname "Titus" has age 98 has identifier "Mary Melissa Titus" has middlename "Melissa" has gender "female" has birth-date "1847-08-12";
insert $p0 has death-date "1872-03-06" has age 74 has identifier "John Niesz" isa person has birth-date "1798-01-02" has firstname "John" has gender "male" has surname "Niesz";
insert $p0 has identifier "Mary Young" has birth-date "1798-04-09" isa person has firstname "Mary" has death-date "1868-10-28" has surname "Young" has gender "female" has age 70;
# ...
```

#### births.csv

Each row of *births.csv* records a parent and child, with two rows for each of the three children listed:

```csv
parent,child
Timothy Titus,William Sanford Titus
Mary Guthrie,	William Sanford Titus
John Niesz,Elizabeth Niesz
Mary Young,Elizabeth Niesz
Elizabeth Niesz,Mary Melissa Titus
William Sanford Titus,Mary Melissa Titus
...
```

The Graql template code for the Grakn migrator is as follows:

```graql-template
match
	$c isa person has identifier <child>;
	$p isa person has identifier <parent>;
insert
	(child: $c, parent: $p) isa parentship;

```

For each row in the CSV file, the template matches the child and parent cells to their corresponding `person` entities, and then inserts a `parentship` relation, placing the entities it has matched into the `child` and `parent` roles.

{% include note.html content="You must ensure that all entities exist in a graph before inserting relations." %}

Calling the Grakn migrator on the *births.csv* file using the above template (named `births-migrator.gql`) is performed as follows:

```bash
<relative-path-to-Grakn>/bin/migration.sh csv -i ./births.csv -t ./migrators/births-migrator.gql -k grakn
```

The data insertion generated by the migrator is as follows:

```graql
match $c0 has identifier "William Sanford Titus" isa person; $p0 isa person has identifier "Timothy Titus";
insert (child: $c0, parent: $p0) isa parentship;
match $p0 isa person has identifier "Mary Guthrie"; $c0 has identifier "William Sanford Titus" isa person;
insert (child: $c0, parent: $p0) isa parentship;
match $p0 isa person has identifier "Elizabeth Niesz"; $c0 isa person has identifier "Mary Melissa Titus";
insert (child: $c0, parent: $p0) isa parentship;
match $c0 isa person has identifier "Mary Melissa Titus"; $p0 has identifier "William Sanford Titus" isa person;
insert (child: $c0, parent: $p0) isa parentship;
match $c0 isa person has identifier "Elizabeth Niesz"; $p0 has identifier "John Niesz" isa person;
insert (child: $c0, parent: $p0) isa parentship;
match $c0 isa person has identifier "Elizabeth Niesz"; $p0 has identifier "Mary Young" isa person;
insert (child: $c0, parent: $p0) isa parentship;
# ...
```

#### weddings.csv

The *weddings.csv* file contains two columns that correspond to both spouses in a marriage, and an optional column for a photograph of the happy couple:

```csv
spouse1,spouse2,picture
Timothy Titus,Mary Guthrie,
John Niesz,Mary Young,http://1.bp.blogspot.com/-Ty9Ox8v7LUw/VKoGzIlsMII/AAAAAAAAAZw/UtkUvrujvBQ/s1600/johnandmary.jpg
Elizabeth Niesz,William Sanford Titus,
```

The Graql template code for the migrator is as follows:

```graql-template
match
	$x has identifier <spouse1>;
	$y has identifier <spouse2>;
insert
	(spouse1: $x, spouse2: $y) isa marriage

	if (<picture> != "") do 
		{
		has picture <picture>
		};
```

For each row in the CSV file, the template matches the two spouse cells to their corresponding `person` entities, and then inserts a `marriage` relation, placing the entities it has matched into the `spouse1` and `spouse2` roles. If there is data in the picture cell, a `picture` resource is also created for the `marriage` relation.

Calling the Grakn migrator on the *weddings.csv* file using the above template (named `weddings-migrator.gql`) is performed as follows:

```bash
<relative-path-to-Grakn>/bin/migration.sh csv -i ./weddings.csv -t ./migrators/weddings-migrator.gql -k grakn
```

The Graql insertion code is as follows:

```graql
match $x0 has identifier "Timothy Titus"; $y0 has identifier "Mary Guthrie";
insert (spouse1: $x0, spouse2: $y0) isa marriage;
match $x0 has identifier "John Niesz"; $y0 has identifier "Mary Young";
insert has picture "http:\/\/1.bp.blogspot.com\/-Ty9Ox8v7LUw\/VKoGzIlsMII\/AAAAAAAAAZw\/UtkUvrujvBQ\/s1600\/johnandmary.jpg" (spouse1: $x0, spouse2: $y0) isa marriage;
match $y0 has identifier "William Sanford Titus"; $x0 has identifier "Elizabeth Niesz";
insert (spouse1: $x0, spouse2: $y0) isa marriage;
# ...
```


## Migration Script

For simplicity, the */raw-data/* directory of the example project contains a script called *loader.sh* that calls each migration script in turn, so you can simply call the script from the terminal, passing in the path to the Grakn */bin/* directory.

```bash
 ./loader.sh <relative-path-to-Grakn>/bin
```

The migration will take a minute or two, and the terminal will report which file it is migrating at each step. When it is complete, it will report that it is "Done migrating data". To check that it has been successful, open the [Grakn visualiser](../grakn-dashboard/visualiser.html) and select Types, then Entities, and choose one of those presented to you (the entities should be those described [above](./CSV-migration.html#entities)]. The visualiser will display the entities it has imported. The screenshot below illustrates the result from selecting to see all `person` entities.

![Person query](/images/match-$x-isa-person.png)

We have completed the data import, and the graph can now be queried. For example, from the Graql shell:

```graql
match $x isa person, has identifier $i; aggregate count;
```

There should be 60 people in the dataset.

## Data Export

In this example, we have imported a dataset stored in three separate CSV files into Grakn to build a simple graph. We have discussed the ontology and migration templates, and shown how to apply the templates to the CSV data using the shell migrator, using a script file *loader.sh* to automate calling the migrator on each file It is possible to export the data from Grakn, in *.gql* format, so that it can easily be loaded to a graph again without the need to migrate from CSV.  

To export the data, we use the Grakn *migration.sh* shell script again, as described in the [migration documentation](../migration/migration-overview.html#export-from-grakn):

```bash
# Export the ontology
<relative-path-to-Grakn>/migration.sh export -ontology > ontology-export.gql

# Export the data
<relative-path-to-Grakn>/migration.sh export -data > data-export.gql
```

Exporting data or the ontology from Grakn, into Graql, will always redirect to standard out, so above we are sending the output to an appropriately named file.

## Where Next?

This example has illustrated how to migrate CSV data into Grakn. Having read it, you may want to further study our documentation about [CSV migration](../migration/CSV-migration.html) and [Graql templating](https://grakn.ai/pages/documentation/graql/graql-templating.html).  

{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/27" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

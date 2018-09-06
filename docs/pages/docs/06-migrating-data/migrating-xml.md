---
title: XML Migration to Grakn
keywords: setup, getting started
tags: [migration]
summary: "This document will teach you how to migrate XML data into Grakn."
sidebar: documentation_sidebar
permalink: /docs/migrating-data/migrating-xml
folder: docs
KB: plants
---

## Introduction

This is reference documenation for how to migrate XML data into grakn. Make sure you have set up the [Grakn environment](../get-started/setup-guide) before continuing.

## Migration Shell Script for XML

The migration shell script can be found in _/bin_ directory of your Grakn environment. We will illustrate its usage in an example below:

```bash
usage: graql migrate xml -template <arg> -input <arg> -keyspace <arg> -element <arg> -schema <arg> [-help] [-no] [-batch <arg>] [-active <arg>] [-uri <arg>] [-retry <arg>] [-verbose]

OPTIONS
 -a,--active <arg>     Number of tasks (batches) running on the server at
                       any one time. Default 25.
 -b,--batch <arg>      Number of rows to execute in one Grakn transaction.
                       Default 25.
 -c,--config <arg>     Configuration file.
 -e,--element <arg>    The name of the XML element to migrate - all others
                       will be ignored.
 -h,--help             Print usage message.
 -i,--input <arg>      Input XML data file or directory.
 -k,--keyspace <arg>   Grakn knowledge graph. Required.
 -n,--no               Write to standard out.
 -r,--retry <arg>      Retry sending tasks if engine is not available
 -s,--schema <arg>     The XML Schema file name, usually .xsd extension
                       defining with type information about the data.
 -t,--template <arg>   Graql template to apply to the data.
 -u,--uri <arg>        Location of Grakn Engine.
 -v,--verbose          Print counts of migrated data.
 -d,--debug            Migration immediatly stops if any transaction fails
```

## XML Migration Basics

The steps to migrate XML data to GRAKN.AI are:

- define a schema for the data to derive the full benefit of a knowledge graph
- create templated Graql to map the XML data to the schema
- invoke the Grakn migrator through the shell script or Java API. The XML migrator will apply the template to each instance of the specified element `-element`, replacing the sections indicated in the template with provided data: the XML tags are the key to be used in the brackets `<>` and the content of each tag are the value of that key.

{% include note.html content="XML Migration makes heavy use of the Graql templating language. You will need a foundation in Graql templating before continuing, so please read through our [migration langauge documentatino](../migrating-data/migration-language) to find out more." %}

## XML Migration Details

TO work through

```xml
<PEOPLE>
    <PERSON BESTFRIEND="Bob">
        This person has many friends.
        <BESTFRIEND>Charlie</BESTFRIEND>
        <BESTFRIEND NAME="Alice"/>
    </PERSON>
</PEOPLE>
```

**Note: the above is _very_ bad XML and is only being used to illustrate the reaches of the migration language**

Any input XML data will be converted into a format that Grakn migration understands, which is structurally similar to JSON.

```json
{
    "PERSON": [
        {
            "textContent"="This person has many friends.",
            "~BESTFRIEND"="Bob",
            "BESTFRIEND"=[
                {"textContent"="Charlie"},
                {"~NAME"="Alice"}
            ]
        }
    ]
}
```

This is the Grakn migration representation of the given XML data.

### XML Nodes and Text Content

Nodes can be migrated using their tags as the keys in the template. To migrate that the example person has best friend "Charlie" you only need to specify the `BESTFRIEND` node. However, you also need to indicate that you want the inner text of that node using the `textContent` indicator.

```graql-skip-test
insert $x isa person has name <PERSON[0].textContent>;
```

To refer to the content of a node that is the current context of the template we can use only the `textContent` indicator.

```graql-skip-test
for(<PERSON>) do {
    insert $x isa person has description <textContent>;
}
```

### XML Attributes

Attributes are prefixed by `~` to differentate them from tags of the same name.

```graql-skip-test
insert $x isa person has best-friend <"~BESTFRIEND">;
```

Note that any non-alphanumeric key needs to be in quotes.

This also works with nesting. To migrate that this person has a bestfriend "Alice" you can use the `~` and an array accessor:

```graql-skip-test
insert $x isa person has best-friend <BESTFRIEND[1]."~NAME">;
```

We use an array accessor `[1]` to get the second `BESTFRIEND` element using and extract their name using the `~` to get the value of the `NAME` attribute.

### XML Schemas

An XML schema is a set of descriptors that can be used to formally constrain the elements of an XML document. Providing a XML schema allows the Grakn migrator to make assumptions about the XML data not possible otherwise.

To learn more about how to write an XML Schema, see the [W3 documentation](https://www.w3schools.com/xml/schema_intro.asp).

#### Data Types

There are three supported types that can be extracted from an XML schema.

- boolean (xml: `type="xs:boolean"`)
- int (xml: `type="xs:integer"`)
- double (xml: `type="xs:double"`)

Without a schema all values are considered as strings in the Grakn representation. By specifying the type of the data in the XML schema, you can avoid doing manual conversions in the templates.

We do not currently support the common `date` type. To convert a string value into a Grakn date, see the migration language [date macro](../migrating-data/migration-language).

#### Arrays

Without provding a schema the XML migrator will assume that there could be more than one child with the same node tag and will represent it as an array.

Take the data:

```xml
<pets>
    <cat>Larry</cat>
</pets>
```

Without a schema, the Grakn representation will assume there can be more than one child and will use an array to represent the `cats` node.

```json
{
  "pets": [
    {
      "cat": [
        {
          "textContent": "Larry"
        }
      ]
    }
  ]
}
```

Providing an XML schema you can specify restaints that cannot be inferred from context. This allows you to specify that there will only be one name per person.

```xml
<xs:element name="pets">
  <xs:complexType>
      <xs:element name="cat" type="xs:string" maxOccurs="1"/>
  </xs:complexType>
</xs:element>
```

These XML restraints allow the Grakn representation to be contrained as follows:

```json
{
  "pets": [
    {
      "cat": {
        "textContent": "Larry"
      }
    }
  ]
}
```

## Example: Plants

This example will migrate a single plant entity, along with various reosurces, from XML data into Grakn. This is a snippet of the XML data ([full example](https://www.w3schools.com/xml/plant_catalog.xml)):

```xml
<CATALOG>
    <PLANT>
        <COMMON>Bloodroot</COMMON>
        <BOTANICAL>Sanguinaria canadensis</BOTANICAL>
        <ZONE>4</ZONE>
        <LIGHT>Mostly Shady</LIGHT>
        <PRICE>$2.44</PRICE>
        <AVAILABILITY>031599</AVAILABILITY>
    </PLANT>
    <PLANT>
        <COMMON>Columbine</COMMON>
        <BOTANICAL>Aquilegia canadensis</BOTANICAL>
        <ZONE>3</ZONE>
        <LIGHT>Mostly Shady</LIGHT>
        <PRICE>$9.37</PRICE>
        <AVAILABILITY>030699</AVAILABILITY>
    </PLANT>
    <PLANT>
        <COMMON>Marsh Marigold</COMMON>
        <BOTANICAL>Caltha palustris</BOTANICAL>
        <ZONE>4</ZONE>
        <LIGHT>Mostly Sunny</LIGHT>
        <PRICE>$6.81</PRICE>
        <AVAILABILITY>051799</AVAILABILITY>
    </PLANT>
    ...
</CATALOG>
```

Our schema contains a single plant entity and various resources that are associated with it:

```graql-skip-test
insert

plant sub entity
    has common
    has botanical
    has zone
    has light
    has price
    has availability;

name sub attribute datatype string;
common sub name;
botanical sub name;
zone sub attribute datatype string;
light sub attribute datatype string;
price sub attribute datatype double;
availability sub attribute datatype long;
```

We want to insert one `plant` entity in the knowledge graph per `PLANT` tag in the XML data. This means that we must specify main element of the XML migration to be "PLANT". In the migration script you would do so by adding the parameter: `-element PLANT`.

The XML template will be applied to each of the specified elements:

```graql-skip-test
insert

$plant isa plant
    has common <COMMON[0].textContent>
    has botanical <BOTANICAL[0].textContent>
    has zone <ZONE[0].textContent>
    has light <LIGHT[0].textContent>
    has availability @long(<AVAILABILITY[0].textContent>);
```

This template contains a single insert query. As it will be applied to each element of the XML, you will have one insert query per `PLANT` element.

The tirangle brackets are used to specify nested elements: in the XML `<BOTANICAL>` is a direct child of `<PLANT>` and so can be used whenever the template traversal is in the `PLANT` scope.

Before running any migration you need to ensure that the schema has been loaded to the knowledge graph:

```
./graql console -f plants-schema.gql -k plants
```

At this point you can run the XML migration script on the resources we have described above:

```
./graql migrate xml -i plants.xml -t plants-template.gql -e PLANT -k plants
```

Adding the `-n` tag to the migration script will print the resolved graql statements to system out. This is a useful tactic to get used to the Graql langauge and know exactly what is being inserted into your knowledge graph:

```graql
insert $plant0 isa plant has common "Bloodroot" has botanical "Sanguinaria canadensis" has zone "4" has light "Mostly Shady" has availability 31599;
insert $plant0 isa plant has common "Columbine" has botanical "Aquilegia canadensis" has zone "3" has light "Mostly Shady" has availability 30699;
insert $plant0 isa plant has common "Marsh Marigold" has botanical "Caltha palustris" has zone "4" has light "Mostly Sunny" has availability 51799;
```

## Where Next?

You can find further documentation about migration in our API reference documentation (which is in the _/docs_ directory of the distribution zip file, and also online [here](http://javadoc.io/doc/ai.grakn/grakn). An example of JSON migration using the Java API can be found on [Github](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-giphy).

---
title: Migrating structured data - XML and JSON files
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will deepen your knowledge of GRAQL templating and learn about loading non-tabular data files, like XML and JSON files.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/xml-migration.html
folder: overview
toc: false
KB: academy
---

Data stored into CSV files is particularly easy to migrate, because it has a nice and clean tabular format. In this lesson, we will have a quick glance at how it is possible to migrate into GRAKN more structured data files, like XML or JSON files.

We will cover here only some basic constructs that can be used to migrate XML files to avoid this course becoming a full course on XML, which is a very rich topic on its own, but if you are interested in studying more, [the internet is your friend](https://www.w3schools.com/xml/xml_whatis.asp).

## XML data
Data in an XML file is organised in what is called a tree-like structure, like the structure of the folders in your file system: every element can contain several other elements and they can contain other elements as well.

In XML, to denote the beginning of an element, we use an XML tag (that looks like `<ELEMENT>`), while to denote the end of the same element we use a closing tag (that looks like `</ELEMENT>`). An XML file, thus, can look more or less like the following:

```
<ROOT>
<LEVEL1>
    <LEVEL2>
Content
    </LEVEL2>
</LEVEL1>
<LEVEL1>
    <LEVEL2>
    Other Content
    </LEVEL2>
    <NESTED>
        <LEVEL3>
        Something
        </LEVEL3>
    </NESTED>
    <NESTED>
        <LEVEL3>
        Something Else
        </LEVEL3>
    </NESTED>
</LEVEL1>
</ROOT>
```

To navigate the tree structure in GRAQL, we use the standard dot notation. This means that, for example, if we want to refer to the content of one of the elements "Level 2" in the file above, we would use `<LEVEL1.LEVEL2>` in our template file (exactly like we used the column names in the CSV templates.

## Loops
You probably have noticed from the example above that an XML element can contain several elements of the same type. What can we do if we want to access the content of all of them?

We need a "for" loop.

The syntax of a "for" loop is very similar to that of an "if" statement:

```graql-skip-test
...
for (<NESTED> do {
  $x has <LEVEL3>
}
...
```

The template bit above (WARNING: that is not a complete and valid template) will loop each element `<NESTED>`, then fetch the content of the tag `<LEVEL3>` within and put it into the GRAQL statement within curly braces.

It might sound complicated, but it honestly just requires a bit of practice and familiarity with the XML format.

## Loading XML files
Loading a GRAQL template against an XML file is a very similar process to the one you learned in the last lesson, but it requires a couple of extra arguments:

While dealing with XML files you often find that the actually interesting stuff only starts after two or three levels of nesting. In the fake example above, for instance, we are only interested in the content of "LEVEL1" elements.

We will then call our migration command with the option `-e` that tells GRAKN what to consider the *base element* of the file. In a way, you can think of it like splitting the XML file into many separate files, each containing one single "LEVEL1" element.

XML files usually come with a schema, which is stored in a XSD file. The schema describes formally the structure of the XML file and it is used by GRAKN to migrate the XML file (for example in a schema for the XML mock above you would find the information that each "LEVEL1" element can contain more that one "NESTED" element.

To refer to the schema while calling the migration command, use the `-s` option.

The command to migrate a template against an XML file, thus, looks like this:

```
graql migrate xml -i INPUT_FILE.xml -s SCHEMA_FILE.xsd -e BASE_ELEMENT -t TEMPLATE.gql -k KEYSPACE
```

## Putting it all together
In your VM, in the folder `academy/short-training/data` you will find an xml file called `bonds.xml` with its schema called `bonds.xsd` you can also visualise them [here](https://github.com/graknlabs/academy/tree/master/short-training/data). Examine both of them carefully and then have a look at the following template (that is stored as the `bonds-template.gql` file).

```graql-template
match $issuer isa company has name <issuerName>;
insert
for (<bonds.bond>) do {
  $bond isa bond has name <bondName>
  has risk @double(<bondRisk>);
  (issuer: $issuer, issued: $bond) isa issues;
}
```

What do you think the template will do?

How would you migrate the xml file using "bondIssuer" as the _base element_?


## What about JSON?

There are no specific JSON examples in the Academy, as their migration works just as the migration of XML files. The only differences are:

  * The migration command is `graql migrate json` instead of `graql migrate xml`
  * The schema is not used
  * There is no _base element_ for the JSON migration

### What have you learned?
Whew! That was a lot to take in! You should by know have learned the basic of data migration into GRAKN. Be sure to have understood well how to issue the migration commands for XML files, because you will need them in the module review.

## What next
You are almost there! After the module [review](./migration-review.html) you will have put all the data into your knowledge graph and you will be able to proceed to one of the most exciting topics of the academy: logic inference.
If you want to go deeper into the topic of data migration head to the [docs](../index.html).

---
title: An Example of Migrating JSON to Grakn
keywords: migration
tags: [migration, examples]
summary: "An example to illustrate migration of JSON to Grakn"
sidebar: documentation_sidebar
permalink: /docs/examples/JSON-migration
folder: docs
---


## Introduction

This example looks at the migration of simple JSON data to build a knowledge base in GRAKN.AI. The data is an invented library catalogue of a very small number of books, and illustrates how to migrate simple data and handle issues such as preventing the creation of duplicate entities. You can find the data, schema and templates for this example in our [sample projects repo](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-library) on Github.

For a detailed overview of CSV migration, we recommend that you take a look at the Grakn documentation on [JSON Migration](../migrating-data/migrating-json) and [Graql migration language](../migrating-data/migration-language).  

## Data

The JSON data for the book catalogue is as follows:

```json

{
    "books": [
        {
            "title":"The Gruffalo",
            "id":"1",
            "author":"Julia Donaldson",
            "subject":"Children's Fiction"
        },
        {
            "title":"The Tear Thief",
            "id":"2",
            "author":"Carol Ann Duffy",
            "subject":"Children's Fiction"
        },
        {
            "title":"Selected Poems",
            "id":"3",
            "author":"Carol Ann Duffy",
            "subject":"Poetry"
        },
        ...
    ]
}
```

The `id` value can be considered to be unique and identify each book. However, other values cannot be assumed to be unique. In the snippet shown above, note that the author `Carol Ann Duffy` and subject `Children's Fiction` are both duplicated. It is possible that a book title may also be duplicated, although this is not shown.

### Schema

The schema for the book catalogue is as follows:

```graql
define

# Entities

book sub entity
    has bookId
    has title
    plays publication-item;

author sub entity
    has authorName
    plays publication-author;

subject sub entity
    has subjectName
    plays publication-subject;


# Resources

bookId sub attribute datatype string;
title sub attribute datatype string;
authorName sub attribute datatype string;
subjectName sub attribute datatype string;

# Relations and Roles

publication sub relationship
    relates publication-item
    relates publication-author
    relates publication-subject;
```

Here, there are three entities, to reflect the book, author of the book and possible book subjects. There is one relationship, `publication` which between all three entities.

To load *schema.gql* into Grakn, make sure the engine is running and choose a clean keyspace in which to work (here we use the default keyspace, so we are cleaning it before we get started).

```bash
./grakn server clean
./grakn server start
./graql console -f ./schema.gql
```


### Data Migration

Having loaded the schema, the next steps are to populate the knowledge base by migrating data into Grakn, using Graql templates. There are separate templates for each entity. First, for the books:

```graql-template
insert

for(<books>) do {
    $book isa book
        has bookId <id>
        has title <title>;
}
```

The authors:

```graql-template
insert

for(<books>) do {
    $author isa author
        has authorName <author>;
}
```

The subjects:

```graql-template
insert

for(<books>) do {
    $subject isa subject
        has subjectName <subject>;
}
```

Finally, a template to build the relationships between the entities:


```graql-template

```

To call the migration script on each template:

```bash
./graql migrate json -t ./json/book-template.gql  -i ./json/library-data.json -k grakn
./graql migrate json -t ./json/author-template.gql  -i ./json/library-data.json -k grakn
./graql migrate json -t ./json/subject-template.gql  -i ./json/library-data.json -k grakn
./graql migrate json -t ./json/publication-template.gql  -i ./json/library-data.json -k grakn
```

The resultant Graql for the migration:

<!-- TODO -->
```graql-test-ignore

```


## Visualise the Data

## Add Borrower Data

## Data Export


```bash
# Export the schema
./graql migrate export -schema > schema-export.gql

# Export the data
./graql migrate export -data > data-export.gql
```

Exporting data or the schema from Grakn, into Graql, will always redirect to standard out, so above we are sending the output to an appropriately named file.

## Where Next?

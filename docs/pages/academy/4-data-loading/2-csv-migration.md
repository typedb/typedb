---
title: Migrating CSV files - Graql templates
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn about Graql templating and how to migrate data from CSV files into Grakn.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/csv-migration.html
folder: overview
toc: false
KB: academy
---

In the last lesson, you should have learned how to load a file containing a (potentially very long) list of Graql statements into Grakn.

Since it is very likely that if you are migrating a pre-existing database to Grakn, your data will not be in stored as Graql files, we need a way to a way to load some more common file formats, like CSV, into our knowledge base .

To do this, we some more power added to Graql. Meet the Graql templating language.

## Templates
A template file is just a file written in Graql (with some added features) that acts as a filter: you "pour" your file through it and out comes Grakn digestible data.

Let’s write a template to migrate oil platforms into our knowledge base. First of all have a look at how the `platfrom.csv` file looks like (you can find the file [here](https://github.com/graknlabs/academy/blob/master/short-training/data/platforms.csv)). At its core, a CSV file is nothing more than a table: the first line contains the header, with the column names (in this case ID, DistCoast, Country and BelongsTo). The lines after the first contain the data separated by commas (or sometimes some other characters).

A Graql template file looks as simple as this:

```graql-template
insert $x isa oil-platform has platform-id <ID>
has distance-from-coast <DistCoast>;
```

This is nothing more than a simple Graql statement with the added variables in angular brackets, that contain some of the column names of the CSV file.

When you try and load the CSV file using this template (we’ll see how in a short while), Grakn scans every line of the file and produces a Graql statement substituting the column names with the appropriate value and batch load it.

For example, if the line currently being scanned reads

```
13,24,Italy,ENI
```

Our template will be produce the statement

```graql-skip-test
insert $x isa oil-platform has platform-id "13"
has distance-from-coast "24";
```


## Flow control
If you have looked carefully at the CSV file containing the information about the oil platform, you have probably noticed that the value of DistCoast is not always present. If we were to run our current template against the csv, Grakn would try to add distances coast with empty values, and bad things would ensue.

To avoid that, we need to introduce the second Graql extension used in making templates: flow control. More commonly known as "if then" statements. In our templating language, an "if" statement looks like `if (CONDITION) do { STUFF TO BE ADDED }`.
Modify the template like the following:

```graql-template
insert $x isa oil-platform has platform-id <ID>
if (<DistCoast> != "") do {
has distance-from-coast <DistCoast>}
;
```

Let us examine the additions one by one, it’s nothing too hard.

As you know, when running this template against a CSV file, the latter is scanned line by line and each line is converted into a Graql statement. When you add an "if", the content of the curly braces is added to the Graql statement only when the condition within brackets.

The condition to be evaluated is simply a check on the value of one of the columns. In this case `<DistCoast> != ""` means that the value of the column DistCoast is not (that is what `!=` stands for) empty.

Every time the  DistCoast column is empty, then, the Graql statement sent to Grakn will look like this:

```graql
insert $x isa oil-platform has platform-id "123";
```

Notice that the last semicolon is out of the curly braces, so it gets added every time, independently of the value of DistCoast.

## Macros
There is one more thing to add to our template before we can actually use it.
If you read back, you will notice that the example of Graql statement into which the template gets translated looks like

```graql-skip-test
insert $x isa oil-platform has platform-id "13"
has distance-from-coast "24";
```

Noticed the quotes around the 24 (that is, the value of `distance-from-coast`)? This is because every attribute is read as a string, but in our schema we have defined it to be an attribute of datatype long.

If you try and use the template now, Grakn will throw a validation error because you are trying to insert string values into "long" attributes. To solve the issues we need macros.

A macro in Graql is a snippet of code that does some useful data manipulation to help migrate things into your knowledge base. Macros always look like `@MACRO_NAME(ARGUMENT)` where the specific macro is applied to whatever is in brackets. There are several macros that come with the language, but the most used ones are those needed to convert strings into other datatypes (and they are called, not surprisingly, @long, @double, @date and @boolean).

Let’s add our macro to the template:

```graql-template
insert $x isa oil-platform has platform-id <ID>
if (<DistCoast> != "") do {
has distance-from-coast @long(<DistCoast>)}
;
```

That’s all! A lot of words for what is really a slightly modified very simple Graql query! Save your template file as something like `oil-platform-template.gql` and you are ready to use it.


## Migration
To use the template you need the command

`graql migration csv -k KEYSPACE -i INPUT_FILE -t TEMPLATE_FILE`

Try it now in the VM using the template TODO_ADD, the input file TODO_ADD, and the keyspace you created when you loaded the schema during last lesson.

After that, check that oil platforms are in your knowledge base.

  ![Oil Platforms](/images/academy/4-data-loading/oil-platforms.png)

### GOOD TO KNOW:
Why aren’t you using the template file you just wrote and using one stored in the VM instead? That is because the VM is isolated from the rest of the computer (or, as it is called, the host machine). Before being able to use the gql file you created into the VM, you would have to move it from the host machine to the VM. There are many ways of doing that, but are definitely out of the scope of these lessons. Extra points if you manage to find how and to actually migrate the data using the template you have written instead of the one we provided!


### What have you learned?
In this lesson, you have learned about Graql templating language, macros and how to migrate CSV files into Grakn. That was quite a lot, so be sure to have understood all the topics of this lesson before proceeding.

## What next?
[Next lesson](./xml-migration.html) will be about migrating files with a more complex structure than the tabular one of CSV. If you want to delve deeper into the Graql templating language and macros, as usual, head to the [docs](../index.html)

---
title: Pokemon Example
keywords: graql, query
tags: [graql, examples]
summary: "A short example to illustrate Graql queries"
sidebar: documentation_sidebar
permalink: /docs/examples/pokemon
folder: docs
---

This example uses the pokemon schema and dataset, which is available from [github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/pokemon.gql) and is also in the *examples* folder in the Grakn distribution zip. You can use it as a primer to work out if you're building a Graql query correctly, to revise your knowledge or just walk through it as you learn Graql.

First up, you need to load the schema and dataset (which are all in one file) as follows. Here we will use the default keyspace, so will clean it before we start Grakn engine and open the Graql shell.

```bash
./grakn server clean
./grakn server start
./graql console -f <relative-path-to-Grakn>/examples/pokemon.gql
./graql console
```

Here are the queries. Type each one in a separate line and see what happens. Comments are marked with a #, so you can ignore them.

```graql-skip-test
match $x isa pokemon;
match $x has name "Articuno";
match $x val contains "lightning";
match $x has pokedex-no < 20;

# A variable pattern query
match $x isa pokemon;{ $x has name "Mew";} or {($x, $y); $y isa pokemon-type, has name "water"; };

match $x sub concept; # List all concepts
match $x sub attribute; # List all resources
match $x sub entity; # List all entities
match $x sub role; # List all roles
match $x sub relationship; # List all relationships

match $x isa pokemon; (ancestor: $x, $y); $x has name $xn; $y has name $yn;
match evolution relates $x;
match $x plays ancestor;
match $x has name;
match $x plays has-name-owner;
match $x has height = 19.0, has weight > 1500.0; $x has name $name;
match $x has description $desc; $desc val contains "underground";

# regex
match $x val /.*(fast|quick).*/;

#modifiers
match $x isa pokemon, has pokedex-no $no, has name $name; select $name, $no; order by $name asc;
match $x isa pokemon, has pokedex-no $no; select $no; distinct; order by $no asc; offset 50;

# match...insert
match $pk has name "Pikachu"; insert $p isa pokemon, has name "Pichu", has pokedex-no 172; (descendent: $pk, ancestor: $p) isa evolution;
match $p has name "Pichu"; $e has name "electric"; insert (pokemon-with-type: $p, type-of-pokemon: $e) isa has-type;

# insert queries
insert has name "Totodile" isa pokemon;
insert val "Ash" isa name;
insert isa pokemon, has name "Pichu" has height 30;
insert gen2-pokemon sub pokemon;
insert trained-by sub relationship, relates trainer, relates pokemon-trained;
insert pokemon plays pokemon-trained;
insert pokemon has pokedex-no;

# delete queries
match $x has name "Bulbasaur"; delete $x has weight $y;

# aggregate queries
match $x isa pokemon; aggregate count;
match $x isa pokemon, has weight $w; aggregate sum $w;
match $x isa pokemon, has height $h; aggregate max $h;
match $x isa pokemon, has name $n; aggregate min $n;
match $x isa pokemon, has height $h; aggregate average $h;
match $x isa pokemon, has weight $w; aggregate median $w;
match $x isa pokemon-type; $y isa pokemon-type; (attacking-type: $x, defending-type: $y) isa super-effective; aggregate group $x;
match $x isa pokemon, has weight $w, has height $h; aggregate (min $w as minWeight, max $h as maxHeight);


```

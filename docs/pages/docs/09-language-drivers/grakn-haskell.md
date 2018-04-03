---
title: Grakn Haskell Driver
keywords: setup, getting started, download
tags: [getting-started]
summary: "Overview of Grakn Haskell Driver"
sidebar: documentation_sidebar
permalink: /docs/language-drivers/grakn-haskell
folder: docs
---

# Grakn Haskell Client

## Installation

To install the Grakn client, simply add this to your cabal file:

```
build-depends: grakn
```

You will also need access to a Grakn database. Head [here](../get-started/setup-guide.html) to see how to
set up a Grakn database.

## Quickstart

Begin by importing the client:

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Example where

import Grakn

import Data.Function ((&))
```

Define the type labels:

```haskell
person :: Label
person = label "person"

husband :: Label
husband = label "husband"

wife :: Label
wife = label "wife"

marriage :: Label
marriage = label "marriage"
```

Define the variables:

```haskell
x :: Var
x = var "x"

y :: Var
y = var "y"
```

We can translate the following query into Haskell:

```graql
match $x isa person; (husband: $x, wife: $y) isa marriage; get $y;
```

```haskell
query :: GetQuery
query = match
    [ x `isa` person
    , rel [husband .: x, wife .: y] `isa` marriage
    ] & get [y]
```

We can also use infix functions like `(-:)` instead of `isa`:

```haskell
otherQuery :: GetQuery
otherQuery = match
    [ x -: person
    , rel [husband .: x, wife .: y] -: marriage
    ] & get [y]
```

To execute and print the results of our query:

```haskell
client :: Client
client = Client defaultUrl "my-keyspace"

main :: IO ()
main = do
    result <- execute client query
    print result
```
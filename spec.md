# TypeDB - Behaviour Specification

<!-- TODO 
-->

**Table of contents**

- [Foundations](#foundations)
  - [Terminology](#terminology)
  - [The type system](#the-type-system)
    - [Simple (non-dependent) types](#simple-non-dependent-types)
    - [Dependendent types](#dependendent-types)
    - [Castings / Subtypes](#castings--subtypes)
    - [Lists](#lists)
    - [Type operators](#type-operators)
- [Schema](#schema)
  - [Basics of schemas](#basics-of-schemas)
  - [Define semantics](#define-semantics)
    - [Type axioms](#type-axioms)
    - [Constraints](#constraints)
    - [Triggers](#triggers)
    - [Value types](#value-types)
    - [Functions defs](#functions-defs)
  - [Undefine semantics](#undefine-semantics)
    - [Type axioms](#type-axioms-1)
    - [Constraints](#constraints-1)
    - [Triggers](#triggers-1)
    - [Value types](#value-types-1)
    - [Functions defs](#functions-defs-1)
  - [Redefine semantics](#redefine-semantics)
    - [Type axioms](#type-axioms-2)
    - [Constraints](#constraints-2)
    - [Triggers](#triggers-2)
    - [Value types](#value-types-2)
    - [Functions defs](#functions-defs-2)
- [Data instance languages](#data-instance-languages)
  - [Pattern semantics](#pattern-semantics)
    - [Basics: Variables, concept maps, satisfaction](#basics-variables-concept-maps-satisfaction)
    - [Concept satisfaction for patterns of:](#concept-satisfaction-for-patterns-of)
      - [Types](#types)
      - [Constraints](#constraints-3)
      - [Data](#data)
      - [Expressions](#expressions)
      - [Functions](#functions)
      - [Patterns](#patterns)
  - [Match semantics](#match-semantics)
  - [Functions semantics](#functions-semantics)
    - [Function signature, body, operators](#function-signature-body-operators)
    - [Stream-return](#stream-return)
    - [Single-return](#single-return)
    - [Recursion and recursive semantics](#recursion-and-recursive-semantics)
  - [Insert semantics](#insert-semantics)
    - [Basics of inserting](#basics-of-inserting)
    - [Insert statements](#insert-statements)
    - [Optional inserts](#optional-inserts)
    - [Leaf attribute system constraint](#leaf-attribute-system-constraint)
  - [Delete semantics](#delete-semantics)
    - [Basics of deleting](#basics-of-deleting)
    - [Delete statements](#delete-statements)
    - [Clean-up](#clean-up)
  - [Update semantics](#update-semantics)
    - [Basics of updating](#basics-of-updating)
    - [Update statements](#update-statements)
    - [Clean-up](#clean-up-1)
  - [Put semantics](#put-semantics)
- [System execution](#system-execution)
  - [Pipelines](#pipelines)
    - [Basics of clauses](#basics-of-clauses)
      - [Match](#match)
      - [Insert](#insert)
      - [Delete](#delete)
      - [Update](#update)
      - [Put](#put)
      - [Fetch](#fetch)
    - [Basics of operators](#basics-of-operators)
      - [Select](#select)
      - [Deselect](#deselect)
      - [Sort](#sort)
      - [Limit](#limit)
      - [Offset](#offset)
      - [Reduce](#reduce)
  - [Transactions](#transactions)
    - [Basics](#basics)
    - [Snapshots](#snapshots)
    - [Concurrency](#concurrency)
- [Sharding](#sharding)
- [Glossary](#glossary)
  - [Type system](#type-system)
    - [Type](#type)
    - [Schema type](#schema-type)
    - [Value type](#value-type)
    - [Data instance / instance](#data-instance--instance)
    - [Data value / value](#data-value--value)
    - [Attribute instance value / attribute value](#attribute-instance-value--attribute-value)
    - [Data element / element](#data-element--element)
    - [Concept](#concept)
    - [Concept map](#concept-map)
    - [Stream](#stream)
    - [Answer set](#answer-set)
    - [Answer](#answer)
  - [TypeQL syntax](#typeql-syntax)
    - [Schema query](#schema-query)
    - [Data query](#data-query)
    - [Clause / Stream clause](#clause--stream-clause)
    - [Operators / Stream operator](#operators--stream-operator)
    - [Functions](#functions-1)
    - [Statement](#statement)
    - [Pattern](#pattern)
    - [Stream reduction / reduction](#stream-reduction--reduction)
    - [Clause](#clause)
    - [Block](#block)
    - [Suffix](#suffix)
  - [Syntactic Sugar](#syntactic-sugar)
  - [Typing of operators](#typing-of-operators)


# Foundations

## Terminology

This section collects useful abbrevations. See "Glossary" for other commonly used terms.

* **TT** — transaction time (any time within transaction)
* **CT** — commit time (time of committing a transaction to DB)
* **tvar** — type variable (var representing some type)
* **evar** - element variable (var representing some element in some type). 

  Can further distinguish:
  * **svar** - sized variable (element of sized type)
  * **lvar** — list variable (element of list type)

  _Remark_. A key principle of TypeQL declarative language design is that for any a patterns of variable it can be uniquely inferred whether a var is a **tvar** or **evar**, and whether an evar is an **svar** or **lvar**.


## The type system

This section describes the basic **statements** that comprise our type system, and the **rules** that govern the interaction of these statements.

### Simple (non-dependent) types

* **Types**. We write 
  $`A : \mathbf{Type}`$ to mean the statement:
  > $`A`$ is a type. 

  * _Variations_: in general, may replace $`\textbf{Type}`$ by: 
    * $`\mathbf{Ent}`$ (collection of entity types)
    * $`\mathbf{Rel}`$ (collection of relation types)
    * $`\mathbf{Att}`$ (collection of attribute types)
    * $`\mathbf{Itf}`$ (collection of interface types)
  * _Useful abbreviations_:
    * $`\mathbf{Obj} = \mathbf{Ent} + \mathbf{Rel}`$ (collection of object types)
    * $`\mathbf{ERA} = \mathbf{Ent} + \mathbf{Rel} + \mathbf{Att}`$ (collection of ERA types)
  * _Example_: $`\mathsf{Person} : \mathbf{Ent}`$ means $`\mathsf{Person}`$ an entity type.
* **Typing**
  If $`A`$ is a type, then we may write $`a : A`$ to mean:
  > $`a`$ is an element in type $`A`$.

  * _Example_: $`p : \mathsf{Person}`$ means $`p`$ is of type $`\mathsf{Person}`$

  * _Direct typing_: We write $`a :_! A`$ to mean:
    > $`a`$ was declared as an element of $`A`$ by the user (we speak of a ***direct typing***).

    _Remark_. The notion of direct typing might be confusing at first. Mathematically, it is merely an additional statement in our type system. Intuively, you can think of it as a way of keeping track of the _user-provided_ information. A similar remark applies to direct subtyping ($`<_!`$) below.

    * _Direct typing rule_. The statement $`a :_! A`$ implies the statement $`a : A`$. (The converse is not true!)
    * _Example_. $p :_! \mathsf{Child}$ means the user has inserted $`p`$ into the type $`\mathsf{Child}`$. Our type system may derive $`p : \mathsf{Person}`$ from this (but _not_ $`p :_! \mathsf{Person}`$)

### Dependendent types

* **Dependent types**. We write $`A : \mathbf{Type}(I,J,...)`$ to mean:
  > $`A`$ is a type with interface types $`I, J, ...`$.
  
  * _Application_: Writing $A : \mathbf{Type}(I,J,...)$ ***implies*** $`A(x:I, y:J, ...) : \mathbf{Type}`$ whenever we have $`x: I, y: J, ...`$.
  * _Variations_: We may replace $`\mathbf{Type}`$ by $`\mathbf{Rel}`$ or $`\mathbf{Att}`$.
  * _Example_: $`\mathsf{Marriage : \mathbf{Rel}(Spouse)}`$ is a relation type with interface type $`\mathsf{Spouse} : \mathbf{Itf}`$.
* **Dependent typing**.  We write $`a : A(x : I, y : J,...)`$ to mean:
  > The element $`a`$ lives in the type "$`A`$ of $`x`$ (cast as $`I`$), and $`y`$ (cast as $`J`$), and ...".

  * _Notation for grouping interfaces_: We write $`a : A(x : I, y : I)`$ as $`A : A(\{x,y\}:I^2)`$. (Similarly, when $`I`$ appears $`k`$ times in $`A(...)`$, write $`I^k`$)
  * _Role cardinality_: $`|a|_I`$ counts elements in $`\{x_1,...,x_k\} :I^k`$
  * **Example**: $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$. Then $`|m|_{\mathsf{Spouse}} = 2`$.
* **Key properties of dependencies**. (These are some key rules of the type system!)
  * _Combining dependencies_: Given $A : \mathbf{Type}(I)$ and $`A : \mathbf{Type}(J)`$, this ***implies*** $`A : \mathbf{Type}(I,J)`$. In words:
    > If a type separately depends on $`I`$ and on $`J`$, then it may jointly depend on $`I`$ and $`J`$! 

    _Remark_: This applies recursively to types with $`k`$ interfaces.
    * _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$
  * _Weakening dependencies_: Given $`A : \mathbf{Type}(I,J)`$, this ***implies*** $`A : \mathbf{Type}(I)`$. In words:
    > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

    _Remark_: This applies recursively to types with $`k`$ interfaces.
    * _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse^2})`$ implies $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and also $`\mathsf{Marriage} : \mathbf{Rel}`$ (we identify the empty brackets "$`()`$" with no brackets).
  * _Inheriting dependencies_: If $`A : \mathbf{Type}`$, $`B : \mathbf{Type}(I)`$, $`A \leq B`$ and _not_ $A : \mathbf{Type}(J)$ with $`J \leq I`$, then $`A : \mathbf{Type}(I)`$. In words:
    > Dependencies that are not overwritten are inherited

### Castings / Subtypes

* **Casting**. We write $`A \leq B`$ to mean:
  > type casts from $`A`$ to $`B`$ are possible: 
  
  * _Casting rule_: If $`A \leq B`$ and $`a : A`$, then this ***implies*** $`a : B`$.
  * _Transitivity rule_: If $`A \leq B`$ and $`B \leq C`$, then this ***implies*** $`A \leq C`$.
  * _Reflexivity rule_: If $`A : \mathbf{Type}`$ then this **implies** $`A \leq A`$ 

  * _Direct castings_: We write $`A <_! B`$ to mean:
    > A cast from A to B was declared by user (we speak of a ***direct casting*** from A to B).

    * _Direct-to-general rule_: $`A <_! B`$ ***implies*** $`A \leq B`$.
    * _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$
    * _Example_: $`\mathsf{Child} <_! \mathsf{Nameowner}`$
    * _Example_: $`\mathsf{Person} <_! \mathsf{Spouse}`$
  * _Weakening dependencies of terms rule_: If $`a : A(x:I, y:J)`$ then $`a : A(x:I)`$, equivalently: $`A(x:I, y:J) \leq A(x:I)`$. In other words:
    > Elements in $`A(I,J)`$ casts into elements of $`A(I)`$.

    * _Remark_: More generally, this applies for types with $k \leq 0$ interfaces. (In particular, $`A(x:I) \leq A() = A`$)
    * _Example_: If $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$ then both $`m : \mathsf{Marriage}(x:\mathsf{Spouse})`$ and $`m : \mathsf{Marriage}(y:\mathsf{Spouse})`$

  * _"Covariance of dependencies" casting_: Given $`A \leq B`$, $`I \leq J`$ such that $`A : \mathbf{Type}(I)`$ $`B : \mathbf{Type}(J)`$, then $`a : A(x:I)`$ implies $`a : B(x:J)`$. In other words:
    > When $`A`$ casts to $`B`$, and $`I`$ to $`J`$, then $`A(I)`$ casts to $`B(J)`$.

    _Remark_: This applies recursively for types with $`k`$ interfaces.
    * _Example_: If $`m : \mathsf{HeteroMarriage}(x:\mathsf{Husband}, y:\mathsf{Wife})`$ then $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$

  _Notation_: Write $`X(I) \leq Y(J)`$ to mean $`X : \mathbf{Type}(I)`$, $Y : \mathbf{Type}(J)$ and $`X \leq Y`$, $`I \leq J`$.

### Lists

* **List types**. We write $`[A] : \mathbf{Type}`$ to mean
  > the type of $`A`$-lists, i.e. the type which contains lists $`[a_0, a_1, ...]`$ of elements $`a_i : A`$.

  * _Dependency on list types_: We allow $`A : \mathbf{Type}([I])`$, and thus our type system has types $`A(x:[I]) : \mathbf{Type}`$.
    > $`A(x:[I])`$ is a type depending on lists $`x : [I]`$.

    * _Example_: $`\mathsf{FlightPath} : \mathbf{Rel}([\mathsf{Flight}])`$
  * _Dependent list types_: We allow $`[A] : \mathbf{Type}(I)`$, and thus our type system has types $`[A](x:I) : \mathbf{Type}`$.
    > $`[A](x:I)`$ is a type of $`A`$-lists depending on interface $`I`$.

    * _Example_: $`[a,b,c] : [\mathsf{MiddleName}](x : \mathsf{MiddleNameListOwner})`$
  
  * _Direct typing rules_. Two rules relating to user intention/direct typing. 
    * _Direct typing list rule_: Given $`l = [l_0,l_1,...] :_! [A]`$ this implies $`l_i : A`$. In other words:
      > If the user intends a list typing $`l :_! [A]`$ then the list entries $`l_i`$ will be elements in $`A`$.
    * _Direct dependency list rule_: Given $`l = [l_0,l_1,...] : [I]`$ and $`a :_! A(l : [I])`$ implies $`a : A(l_i : I)`$. In other words:
      > If the user intends dependency on a list $`l`$ then this implies dependency on the list's entries $`l_i`$.

  * _List length_: for list $`l : [A]`$ the term $\mathrm{len}(l) : \mathbb{N}$ represents $`l`$'s length.

### Type operators

* **Sum types**. $`A + B`$ — Sum type
* **Product types**. $`A \times B`$ — Product type
* **Type cardinality**.$`|A| : \mathbb{N}`$ — Cardinality of $`A`$

_Remark for nerds: list types are neither sums, nor products, nor polynomials ... they are so-called _inductive_ types!_


# Schema

This section describes valid declarations of _types_ and axioms relating types (_dependencies_ and _type castings_) for the user's data model, as well as _schema constraints_ that can be further imposed. These declarations are subject to a set of _type system properties_ as listed in this section. The section also describes how such declarations can be manipulated after being first declared (undefine, redefine).

## Basics of schemas

* Kinds of definition clauses:
  * `define`: adds **schema type axioms** or **schema constraints**
  * `undefine`: removes axioms or constraints
  * `redefine`: both removes and adds axioms or constraints
* Loose categories for the main schema components:
  * **Type axioms**: comprises user-defined axioms for the type system (types, subtypes, dependencies).
  * **Constraints**: postulated constraints that the database needs to satisfy.
  * **Triggers**: actions to be executed based on database changes.
  * **Value types**: types for primitive and structured values.
  * **Functions**: parametrized query templates ("pre-defined logic")
* For execution and validation of definitions see "Transactionality" section
* Definition clauses can be chained:
  * _Example_: 
  ```
  define A; 
  define B; 
  undefine C; 
  redefine E;
  ```
* **Planned**: statement can be preceded by a match clause
  * e.g. `match P; define A;`
  * _Interpretation_: `A` may contain non-optional **tvar**s bound in `P`; execute define for each results of match.

## Define semantics

`define` clauses comprise _define statements_ which are described in this section.

_Principles._

1. `define` **can be a no-op**: defining the same statement twice is a no-op.

### Type axioms

**Case ENT**
* `entity A` adds $`A : \mathbf{Ent}`$
* `(entity) A sub B` adds $`A : \mathbf{Ent}, A <_! B`$

***System property***: 

1. _Single inheritance_: Cannot have $`A <_! B`$ and $`A <_! C \neq B`$

**Case REL**
* `relation A` adds $`A : \mathbf{Rel}`$
* `(relation) A sub B` adds $`A : \mathbf{Rel}, A <_! B`$, ***requiring*** that $`B : \mathbf{Rel}`$ 
* `(relation) A relates I` adds $`A : \mathbf{Rel}(I)`$ and $`I : \mathbf{Itf}`$.
* `(relation) A relates I as J` adds $`A : \mathbf{Rel}(I)`$, $`I <_! J`$, ***requiring*** that $`B : \mathbf{Rel}(J)`$ and $`A <_! B`$
* `(relation) A relates I[]` adds $`A : \mathbf{Rel}([I])`$
* `(relation) A relates I[] as J[]` adds $`A : \mathbf{Rel}([I])`$, $`I <_! J`$, ***requiring*** that $`B : \mathbf{Rel}([J])`$ and $`A <_! B`$

***System property***: 

1. _Single inheritance_: Cannot have $`A <_! B`$ and $A <_! C \neq B$
2. _Single inheritance (for interfaces)_: Cannot have $`I <_! J`$ and $I <_! K \neq J$ for $`I,J,K :\mathbf{Itf}`$
3. _Exclusive interface modes_: Cannot have both $`A : \mathbf{Rel}(I)`$ and $`A : \mathbf{Rel}([I])`$ (in other words, cannot have both `A relates I` and `A relates I[]`).
4. _Implicit inheritance_: Cannot redeclare inherited interface (i.e. when `B relates I`, `A sub B` we cannot re-declare `A relates I`... this is automatically inherited!)

**Case ATT**
* `attribute A` adds $`A : \mathbf{Att}(O_A)`$ and $`O_A : \mathbf{Itf}`$ ($`O_A`$ being automatically generated ownership interface)
* `(attribute) A value V` adds $`A <_! V`$, ***requiring*** that $`V`$ is a primitive or struct value type
* `(attribute) A sub B` adds $`A : \mathbf{Att}(O_A)`$, $`A <_! B`$ and $`O_A <_! O_B`$, ***requiring*** that $`B : \mathbf{Att}(O_A)`$

***System property***: 

1. _Single inheritance_: Cannot have $A <_! B`$ and $A <_! C \neq B$ for $`A, B, C : \mathbf{Att}`$.

**Case PLAYS**

* `A plays B:I` adds $`A <_! I`$, ***requiring*** that $`B: \mathbf{Rel}(I)`$, $`A :\mathbf{Obj}`$ and not $B \lneq B'$ with $`B': \mathbf{Rel}(I)`$

_Remark_. The last part of the condition ensure that we can only declare `A plays B:I` if `I` is a role directly declared for `B`, and not an inherited role.

**Case OWNS**
* `A owns B` adds $`A <_! O_B`$, ***requiring*** that $`B: \mathbf{Att}(O_B)`$, $`A :\mathbf{Obj}`$
* `A owns B[]` adds $`A <_! O_B`$, ***requiring*** that $`B: \mathbf{Att}(O_B)`$, **puts B[] to be non-abstract**: i.e. allows declaring terms $`l :_! [B](x:O_B)`$, see earlier discussion of list types

_Remark: based on recent discussion, `A owns B[]` _implies_ `A owns B @abstract` (abstractness is crucial here, see `abstract` constraint below). See also the remark in "Satisfying type patterns"._

***System property***: 

1. _Exclusive interface modes_: Only one of `A owns B` or `A owns B[]` can be declared in the model.
2. _Consistent interface modes_: If `A owns B`, and $`A' \leq A`$, $`B' \leq B`$, then disallow declaring `A' owns B'[]`.
3. _Consistent interface modes (list case)_: If `A owns B[]`, and $`A' \leq A`$, $`B' \leq B`$, then disallow declaring `A' owns B'`.

### Constraints

**Case CARD**
* `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $`a :_! A'(\{...\} : I^k)`$, $`A' \leq A`$, $`A' : \mathbf{Rel}(I)`$.
  * **defaults** to `@card(1..1)` if omitted ("one")
* `A plays B:I @card(n..m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B @card(n...m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..1)` if omitted ("one or null")

***System property***:

1. For inherited interfaces, we cannot redeclare cardinality (this is actually a consequence of "Implicit inheritance" above). 
2. When we have direct subinterfaces $`I_i <_! J`$, for $`i = 1,...,n`$, and each $`I_i`$ has `card(`$`n_i,m_i`$`)` while J has $`card(n,m)`$ then we must have $`n \leq \sum_i n_i \leq \sum_i m_i \leq m`$.
  
_Remark 1: Upper bounds can be omitted, writing `@card(2..)`, to allow for arbitrary large cardinalities_

_Remark 2: For cardinality, and for most other constraints, we should reject redundant conditions, such as `A owns B card(0..3);` when `A sub A'` and `A' owns B card(1..2);`_

**Case CARD_LIST**
* `A relates I[] @card(n..m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`a : A'(l : [I])`$, $A' \leq A$, $`A' : \mathbf{Rel}([I])`$, and $`k`$ is _maximal_ (for fixed $a : A$).
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B[] @card(n...m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`l : [B](a:O_B)`$ for $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")

**Case PLAYS_AS**
* `A plays B:I as C:J` postulates $`c :_! C(a:J)`$ is impossible when $`a:A`$, ***requiring*** that $B \lneq C$, $`A \leq D`$, $`D <_! J`$.
  * **Invalidated** when $`A <_! J'`$ for $`B(I) \lneq C'(J') \leq C(J)`$.

**Case OWNS_AS**
* `A owns B as C` postulates $`c :_! C(a:O_C)`$ is impossible when $`a:A`$, ***requiring*** that $B \lneq C$, $`A \leq D`$, $`D <_! O_C`$.
  * **Invalidated** when $`A <_! O_{C'}`$ for $`B \lneq C' \leq C`$.

_Comment: both preceding cases are kinda complicated/unnatural ... as reflected by the math._

**Case UNIQUE**
* `A owns B @unique` postulates that if $`b : B(a:O_B)`$ for some $`a : A`$ then this $`a`$ is unique (for fixed $`b`$).

**Case KEY**
* `A owns B @key` postulates that if $`b : B(a:O_B)`$ for some $`a : A`$ then this $`a`$ is unique, and also $`|B(a:O_B) = 1`$.

**Case SUBKEY**
* `A owns B1 @subkey(<LABEL>); A owns B2 @subkey(<LABEL>)` postulates that if $`b : B_1(a:O_{B_1}) \times B_2(a:O_{B_2})`$ for some $`a : A`$ then this $`a`$ is unique, and also $`|B_1(a:O_{B_1}) \times B_2(a:O_{B_2})| = 1`$. **Generalizes** to $`n`$ subkeys.

**Case ABSTRACT**
* `(type) A @abstract` postulates $`a :_! A(...)`$ to be impossible
* `B relates I @abstract` postulates $`A <_! I`$ to be impossible for $`A : \mathbf{Obj}`$
* `B relates I[] @abstract` postulates $`A <_! I`$ to be impossible for $`A : \mathbf{Obj}`$
* `A plays B:I @abstract` postulates that
  *  (if $`I`$ is used as a plain role:) $`b :_! B'(a:I)`$ 
  *  (if $`I`$ is used as a list role:) $`b :_! B'(l:[I])`$, $a \in l$ 
  
  is impossible whenever $`a : A`$, $B' \leq B$ (_note_: $`B' \leq B`$ is needed here, since the interface $`I`$ may be inherited to some subtypes)
* `A owns B @abstract` postulates $`b :_! B(a:I)`$ to be impossible for $`a : A`$ 
* `A owns B[] @abstract` postulates $`b :_! [B](a:I)`$ to be impossible for $`a : A`$ 

***System property***:

> _The following properties capture that parents of abstract things are meant to be abstract too. But this is not really a crucial condition. (STICKY: discuss!)_ 

1. If `(type) A @abstract` and $`A \leq B`$ then `(type) B` cannot be non-abstract.
2. If `A relates I @abstract` and $`A(I) \leq B(J)`$ then `B relates J` cannot be non-abstract.
3. If `A relates I[] @abstract` and $`A([I]) \leq B([J])`$ then `B relates J[]` cannot be non-abstract.
4. If `A plays B:I @abstract` and $`A \leq A'`$, $`B'(I) \leq B'(I')`$ then `A' plays B':J'` cannot be non-abstract.
5. If `A owns B @abstract` and $`A \leq A'`$, $`B \leq B'`$ then `A' owns B'` cannot be non-abstract. 
6. If `A owns B[] @abstract` and $`A \leq A'`$, $`B \leq B'`$ then `A' owns B'[]` cannot be non-abstract. 

**Case VALUES**
* `A owns B @values(v1, v2)` postulates if $`a : A`$ then $`a \in \{v_1, v_2\}`$ , ***requiring*** that 
  * either $`A : \mathbf{Att}`$, $`A \leq V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs. 
  
  **Generalizes** to $`n`$ values.
* `A owns B @regex(v1..v2)` postulates if $`a : A`$ then $`a`$ conforms with regex `<EXPR>`.
* `A owns B @range(v1..v2)` postulates if $`a : A`$ then $`a \in [v_1,v_2]`$ (conditions as before).
* `A value B @values(v1, v2)` postulates if $`a : A`$ then $`a \in \{v_1, v_2\}`$ , ***requiring*** that: 
  * either $`A : \mathbf{Att}`$, $`A \leq V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.
  
  **Generalizes** to $`n`$ values.
* `A value B @regex(v1..v2)` postulates if $`a : A`$ then $`a`$ conforms with regex `<EXPR>`.
* `A value B @range(v1..v2)` postulates if $`a : A`$ then $`a \in [v_1,v_2]`$ (conditions as before).

**Case DISTINCT**
* `A owns B[] @distinct` postulates that when $`[b_1, ..., b_n] : [B]`$ then all $`b_i`$ are distinct. 
* `B relates I[] @distinct` postulates that when $`[x_1, ..., x_n] : [I]`$ then all $`x_i`$ are distinct.

### Triggers

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* `(relation) B relates I @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $`B`$'s cardinality for $`I`$, triggers deletion of $`b`$.
  * **defaults** to **TT** error
* `(relation) B @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $`B`$'s cardinality _for any role_ of $`B`$, triggers deletion of $`b`$.
  * **defaults** to **TT** error
* `(attribute) B @independent`. When deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$, update the latter to $`b :_! B`$.
  * **defaults** to: deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$ triggers deletion of $`b`$.


### Value types

**Case PRIMITIVES**
* `bool`
  * Terms: `true`, `false`
* `long` — _Comment: still think this could be named more nicely_
  * Terms: 64bit integers
* `datetime`
  * TODO: list formats
* `time` — _Comment: relative times_
* `string` — _Comment: dynamically sized type; but could consider Arrow-like implementation, see e.g. [here](https://pola.rs/posts/polars-string-type/)_

**Case STRUCT**
```
struct S:
  C1 value V1 (@values(<EXPR>)),
  C2 value V2? (@values(<EXPR>));
```
adds
* _Struct type_ $`S : \mathbf{Type}`$
* _Struct components_ $`C_1 : \mathbf{Type}`$, $`C_2 : \mathbf{Type}`$, and identify $`S = C_1 \times \mathsf{Opt}(C_2)`$ where $`\mathsf{Opt}`$ denotes the optionality type operator ($`\mathsf{Opt}(T) = T + \{\emptyset\}`$)
    * _Component value casting rule_: $`C_1 \leq V_1`$, $`C_2 \leq \mathsf{Opt}(V_2)`$
    * _Component value constraint rule_: whenever $`v : V_i`$ and $`v`$ conforms with `<EXPR>` then $`v : C_i`$
      * **defaults** to: whenever $`v : V_i`$ then $`v : C_i`$ (no condition)
* **Generalizes** to $`n`$ components

### Functions defs

**Case STREAM_RET_FUN**
```
fun F (x: T, y: S) -> { A, B }:
  match <PATTERN>
  (<OPERATORS>)
  return { z, w };
```
adds the following to our type system:
* _Function symbol_: $`F : \mathbf{Type}(T,S)`$.
* _Function type_: when $`x : T`$ and $`y: S`$ then $`F(x:T, y:S) : \mathbf{Type}`$
* _Output cast_: $`F(x:T, y:S) \leq A \times B`$
* _Function terms_: $`(z,w) : F(x:T, y:S)`$ are discussed in section "Function semantics"
* **Generalizes** to $`n`$ inputs and $`m`$ outputs

**Case SINGLE_RET_FUN**
```
fun f (x: T, y: S) -> A, B:
  match <PATTERN>
  (<OPERATORS>)
  return <AGG>, <AGG>;
```
adds the following to our type system:
* _Function symbol_: when $`x : T`$ and $`y: S`$ then $`f(x:T, y:S) : A \times B`$
* _Function terms_: $`(z,w) : f(x:T, y:S)`$ are discussed in section "Function semantics"
* **Generalizes** to $`n`$ inputs and $`m`$ outputs

_Comment: notice difference in capitalization between the two cases!_

## Undefine semantics

`undefine` clauses comprise _undefine statements_ which are described in this section.

_Principles._

* `undefine` removes axiom, constraints, triggers, value types, or functions
* `undefine` **can be a no-op**

### Type axioms

**Case ENT**
* `entity A` removes $`A : \mathbf{Ent}`$
* `sub B from (entity) A` removes $`A \leq B`$

**Case REL**
* `relation A` removes $`A : \mathbf{Rel}`$
* `sub B from (relation) A` removes $`A \leq B`$
* `relates I from (relation) A` removes $`A : \mathbf{Rel}(I)`$
* `as J from (relation) A relates I` removes $`I <_! J`$ 
* `relates I[] from (relation) A` removes $`A : \mathbf{Rel}([I])$
* `as J[] from (relation) A relates I[]` removes $`I <_! J`$

**Case ATT**
* `attribute A` removes $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$
* `value V from (attribute) A value V` removes $`A \leq V`$
* `sub B from (attribute) A` removes $`A <_! B`$ and $`O_A <_! O_B`$

**Case PLAYS**
* `plays B:I from (type) A` removes $`A <_! I`$ 

**Case OWNS**
* `owns B from (type) A` removes $`A <_! O_B`$ 
* `owns B[] from (type) A` removes $`A <_! O_B`$

### Constraints

_In each case, `undefine` removes the postulated condition (restoring the default)._ (minor exception: subkey)

**Case CARD**
* `@card(n..m) from A relates I`
* `@card(n..m) from A plays B:I`
* `@card(n...m) from A owns B`

**Case CARD_LIST**
* `@card(n..m) from A relates I[]`
* `@card(n...m) from A owns B[]`

**Case PLAYS_AS**
* `as C from A plays B`

**Case OWNS_AS**
* `as C from A owns B`

**Case UNIQUE**
* `@unique from A owns B`

**Case KEY**
* `@key from A owns B`

**Case SUBKEY**
* `@subkey(<LABEL>) from A owns B` removes $`B`$ as part of the `<LABEL>` key of $`A`$

**Case ABSTRACT**
* `@abstract from (type) B` 
* `@abstract from A plays B:I`
* `@abstract from A owns B` 
* `@abstract from A owns B[]` 
* `@abstract from B relates I`
* `@abstract from B relates I[]`

**Case VALUES**
* `@values(v1, v2) from A owns B` 
* `@range(v1..v2) from A owns B`
* `@values(v1, v2) from A value B` 
* `@range(v1..v2) from A value B`

**Case DISTINCT**
* `@distinct from A owns B[]`
* `@distinct from B relates I[]`

### Triggers

_In each case, `undefine` removes the triggered action._

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* `@cascade from (relation) B relates I`
* `@cascade from (relation) B`
* `@independent from (attribute) B`

### Value types

**Case PRIMITIVES**
cannot undefine primitives

**Case STRUCT**

* `struct S;`
  removes $S : \mathbf{Type}$ and all associated defs.
  * **TT** error if
    * $`S`$ is used in another struct
    * $`S`$ is used as value type of an attribute

### Functions defs

**Case STREAM_RET_FUN**
* `fun F;`
  removes $`F`$ and all associated defs.
  * **TT** error if
    * $`S`$ is used in another function


**Case SINGLE_RET_FUN**
* `fun f;`
  removes $`f`$ and all associated defs.
  * **TT** error if
    * $`S`$ is used in another function

_Comment: notice difference in capitalization between the two cases!_

## Redefine semantics

`redefine` clauses comprise _redefine statements_ which are described in this section.

_Principles._

1. `redefine` redefines type axioms, constraints, triggers, structs, or functions
2. Except for few cases (`sub`), `redefine` **cannot be a no-op**, i.e. it always redefines something!
3. _Design principle_: We disallow redefining boolean properties:
  * _Example 1_: a type can either exists or not. we cannot "redefine" it's existence, but only define or undefine it.
  * _Example 2_: a type is either abstract or not. we can only define or undefine `@abstract`.

***System property***: 
1. within a single `redefine` clause we cannot both redefine type axioms _and_ constraint affecting those type axioms
2. _Example_. (TODO)

### Type axioms

**Case ENT**
* cannot redefine `entity A`
* `(entity) A sub B` redefines $`A \leq B`$

**Case REL**
* cannot redefine `relation A` 
* `(relation) A sub B` redefines $`A \leq B`$, ***requiring*** 
  * either $`A <_! B' \neq B`$ (to be redefined)
  * or $`A`$ has no direct super-type
* `(relation) A relates I` redefines $`A : \mathbf{Rel}(I)`$, ***requiring*** that $`A : \mathbf{Rel}([I])`$ (to be redefined)
  * _inherited cardinality_: inherits card (default: `@card(0..)`) 
  * _data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I^k`$
* `(relation) A relates I as J$` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role
* `(relation) A relates I[]` redefines $`A : \mathbf{Rel}([I])`$, ***requiring*** that $`A : \mathbf{Rel}(I)`$ (to be redefined)
  * _inherited cardinality_: inherits card (default: `@card(1..1)`) (STICKY)
  * _data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I^k`$
* `(relation) A relates I[] as J[]` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role

**Case ATT**
* cannot redefine `attribute A`
* `(attribute) A value V` redefines $`A \leq V`$
* cannot redefine `(attribute) A sub B`

**Case PLAYS**
* cannot redefine `(type) A plays B:I`

**Case OWNS**
* cannot redefine `(type) A owns B`
* cannot redefine `(type) A owns B[]`

### Constraints

_In each case, `redefine` redefines the postulated condition._

**Case CARD**
* `A relates I @card(n..m)`
* `A plays B:I @card(n..m)`
* `A owns B @card(n...m)`

**Case CARD_LIST**
* `A relates I[] @card(n..m)`
* `A owns B[] @card(n...m)`

**Case PLAYS_AS**
* `A plays B as C`

**Case OWNS_AS**
* `A owns B as C`

**Case UNIQUE**
* cannot redefine `A owns B @unique`

**Case KEY**
* cannot redefine `A owns B @key`

**Case SUBKEY**
* cannot redefine `A owns B @subkey(<LABEL>)`

**Case ABSTRACT**
* cannot redefine `(type) B @abstract` 
* cannot redefine `A plays B:I @abstract`
* cannot redefine `A owns B @abstract` 
* cannot redefine `A owns B[] @abstract` 
* cannot redefine `B relates I @abstract`
* cannot redefine `B relates I[] @abstract`

**Case VALUES**
* `A owns B @values(v1, v2)` 
* `A owns B @regex(<EXPR>)` 
* `A owns B @range(v1..v2)`
* `A value B @values(v1, v2)` 
* `A value B @regex(<EXPR>)` 
* `A value B @range(v1..v2)`

**Case DISTINCT**
* cannot redefine `A owns B[] @distinct`
* cannot redefine `B relates I[] @distinct`

### Triggers

_In each case, `redefine` redefines the triggered action._

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* cannot redefine `(relation) B relates I @cascade`
* cannot redefine `(relation) B @cascade`
* cannot redefine `(attribute) B @independent`

### Value types

**Case PRIMITIVES**

cannot redefine primitives

**Case STRUCT**

`redefine struct A: ...` replaces the previous definition of `A` with a new on. 

### Functions defs

**Case STREAM_RET_FUN**

`redefine fun F: ...` replaces the previous definition of `F` with a new on. 

**Case SINGLE_RET_FUN**

cannot redefine single-return functions.

# Data instance languages

This section first describes the satisfication semantics of match queries, obtained by substituting _variables_ in _patterns_ by concepts (_answers_) such that these patterns are _satisfied_. It is then described how instance in ERA types can be declared and further manipulated. Finally, the section describes the semantics of functions (the novelty over match semantics is the ability to declared functions recursively).

## Pattern semantics

### Basics: Variables, concept maps, satisfaction

**Variables**

* _Syntax_: vars start with `$`
  * _Examples_: `$x`, `$y`, `$person`
* _Usage_: vars appear as part of 
  * statements: syntactic units of TypeQL (see Glossary)
  * patterns: collection of statements, combined with logical connectives:
    * `;` "and" (default), 
    * `or`, 
    * `not`, 
    * `try`
* _Var kinds_: Position in a statement determines wether variables are
  * type variables (**tvar**, uppercase convention in this spec)
  * data instance variables (**evar**, lowercase convention in this spec)
* _Anon vars_: anon vars start with `$_`. They behave like normal variables, but are automatically discarded (see "Deselect") at the end of the pattern.
  * _Examples_: `$_x`, `$_y`, `$_person`
  * _Implicit naming_. Writing `$_` by itself leaves the name of the anon variable implicit—in this case, a unique name is implicitly chosen (in other words: two `$_` appearing in the same pattern represent different variables)
  * _Remark_: Anon vars can be both **tvar**s and **evar**s

_Remark 1_. The code variable `$x` will be written as $`x`$ in math notation (without $`\$`$).

_Remark 2_. Currently, only implicit named anon vars (`$_`) can be used by the user (under the hood, general anon vars do exist though!). (STICKY)

**Typed concept maps**

* _Concepts_. A **concept** is a type or an element in a type.
* _Typed concept maps_. An **typed concept map** (cmap) $`m`$ is a mapping variables to non-dependently typed concepts
  ```
  m = ($x->a:T, $y->b:S, ...)
  ```
  (math. notation: $(x \mapsto a:T, y \mapsto b:S, ...)$).

  To emphasize: **Types are non-dependent** (i.e. dissallow `$x -> a:T($y:I)`, only allow `$x -> a:T`). 
  * _Assigned concepts_. Write `m($x)` (math. notation $m(x)$) for the concept that `m` assigns to `$x`.
  * _Assigned types_. Write `T($x)` (math. notation $`T_m(x)`$) for the type that `m` assigns to `$x`.
    * _Special case: assigned kinds_. Note that `T($x)` may be `Ent`, `Rel`, `Att`, `Itf` (`Rol`), or `Val` (for value types) when `$x` is assigned a type as a concept — we speak of `T($x)` as the **type kind** of `m($x)` in this case.

**Pattern satisfaction and answers**

* _Pattern satisfaction_. A cmap `m` may **satisfy** a pattern `P`: 
  > Intuitively, this means substituting the variables in `P` with the concepts assigned in `m` yields statements that are true in our type system.
  
  * _Definition_. Satisfication has two requirements: 
    * **concept satisfaction** ("concepts assigned by `m` must conform with pattern `P`)
    * **type satisfication** ("type assigned by `m` must conform with pattern `P`")

    We define concept satisfaction in the _next section_, and only spell out _type satisfaction_ for now:
    1. For any var `$x` we require $`m(x) : T_m(x)`$ to be true in the type system
    1. If `$x isa $A` in `P` then require $`T_m(x) \leq m(A)`$
    1. If `$x links ($B: $y)` in `P` then require $`T_m(y) \leq m(B)`$ and $`T_m(x) : \mathbf{Rel}(m(B))`$
    1. If `$x links ($B[]: $y)` in `P` then require $`T_m(y) \leq m(B)`$ and $`T_m(x) : \mathbf{Rel}([m(B)])`$
    1. If `$x has $B $y` in `P` then require $`T_m(x) \leq O_{m(B)}`$ and
        * either $`T_m(y) \leq m(B)`$
        * or $`T_m(y) = V$ for $V : \mathbf{Val}`$ and $`m(B) <_! V`$
    1. If `$x has $B[] $y` in `P` then require $`T_m(x) \leq O_{m(B)}`$ and
        * either $`T_m(y) \leq [m(B)]`$
        * or $`T_m(y) = [V]`$ for $`V : \mathbf{Val}`$ and $`m(B) <_! V`$
    1. If `$x in $y` in `P` then require $`[T_m(x)] \leq T_m(y)`$
    1. If `$x = <EXPR>` in `P`, then require $`T(\mathrm{expr}) \leq T_m(x)`$ where $`T(\mathrm{expr})`$ is the type of the expression 
        * _Note_: types of expressions can be computed recursively since assignments are acyclic.
    1. If `$x = fun(<VARS>)` or `$x in fun(<VARS>)` in `P`, then require $`T(\mathrm{fun}) \leq T_m(y)`$ where $`T(\mathrm{fun})`$ is the output type of the function 
  
    _Remark_ 
      * In the last to cases, we can replace $\leq$ with $`=`$ to compute the **minimal type assignment** (see "Answers" below).
      * For **tvar**s `$x` we also pick $`m(x) : T_m(x)`$ as minimal as possible by default (e.g. `person : Ent` instead of `person : Type`).
      * The extra cases for `has` are introduced to facilate working with computed values (of potentially non-attribute type) to match attributes.

   * _Replacing **var**s with concepts_. When discussing pattern satisfaction, we always consider **fully variablized** statements (e.g. `$x isa $X`, `$X sub $Y`). This also determines satisfaction of **partially assigned** versions of these statements (e.g. `$x isa A`, `$X sub A`, `A sub $Y`, or `x isa $A`).
<!-- Examples for the typing algorithm:
fun a($x: person) -> name[]:
match
  $x has firstname $f;
  $x has lastname $l;
  $namelist = [$f, $l]; // type as `[$f] + [$l]`
return $namelist;

match
  $x has color $y;
  $x has name $y; // "violet"
  $y isa color;

// 2x2 matrix:
// no:
$y isa name;
$y isa color;
// no:
$x has name $y;
$y isa color;
// no:
$y isa name;
$x has color $y;
// yes:
$x has name $y;
$x has color $y;
// STICKY: are we happy with this?
-->

* _Answers_. A cmap `m` that satisfies a pattern `P` is an **answer** to the pattern if:
  * **The map is minimal** in that no concept map with less variables satisfies `P`
  * **Types are assigned minimally** in that no assignment with more specific types satisfies `P`
  * All variables in `m` are **bound outside a negation** in `P`

_Example_: Consider the pattern `$x isa Person;` (this pattern comprises a single statement). Than `($x -> p)` satisfies the pattern if `p` is an element of the type `Person` (i.e. $p : \mathsf{Person}$). The answer `($x -> p, $y -> p)` also satisfies the pattern, but it is not proper minimal.

**Optional variables**

_Key principle_:

* If variables are used only in specific positions (called **optional positions**) of patterns, then they are optional variables.
  * if a var is used in _any_ non-optional position, then the var become non-optional!
* A optional variable `$x` is allowed to have the empty concept assigned to it in an answer: $`m(x) = \emptyset`$.

**Variable boundedness condition**

_Key principle_:

* A pattern `P` will only be accepted by TypeDB if all variables are **bound**. 
  * (Fun fact: otherwise, we may encounter unbounded/literally impossible computations of answers.)
* A variable is bound if it appears in a _binding position_ of at least one statement. 
  * Most statements bind their variables: in the next section we highlight _non-bound positions_

### Concept satisfaction for patterns of:

Given a cmap `m` and pattern `P` we say `m` ***satisfies*** `P` if:
* it's type assignment satisfies `P` as described in the previous section.
* it's concept assignment satisfies `P` by satisfying _each statement in `P`_ ... as we now describe.

#### Types

**Case TYPE_DEF**
* `Kind $A` (for `Kind` in `{entity, relation, attribute}`) is satisfied if $`m(A) : \mathbf{Kind}`$

* `(Kind) $A sub $B` is satisfied if $`m(A) : \mathbf{Kind}`$, $`m(B) : \mathbf{Kind}`$, $`m(A) \lneq m(B)`$
* `(Kind) $A sub! $B` is satisfied if $`m(A) : \mathbf{Kind}`$, $`m(B) : \mathbf{Kind}`$, $`m(A) <_! m(B)`$

_Remark_: `sub!` is convenient, but could actually be expressed with `sub`, `not`, and `is`. Similar remarks apply to **all** other `!`-variations of TypeQL key words below.

**Case REL_PATT**
* `$A relates $I` is satisfied if $`m(A) : \mathbf{Rel}(m(I))`$

* `$A relates! $I` is satisfied if $`m(A) : \mathbf{Rel}(m(I))`$ and **not** $`m(A) \lneq m(B) : \mathbf{Rel}(m(I))`$
* `$A relates $I as $J` is satisfied if $`m(A) : \mathbf{Rel}(m(I))`$, $`B : \mathbf{Rel}(m(J))`$, $`A \leq B`$, $`m(I) \leq m(J)`$.
* `$A relates $I[]` is satisfied if $`m(A) : \mathbf{Rel}(m([I]))`$
* `$A relates! $I[]` is satisfied if $`m(A) : \mathbf{Rel}(m([I]))`$ and **not** $`m(A) \lneq m(B) : \mathbf{Rel}(m([I]))`$
* `$A relates $I[] as $J[]` is satisfied if $`m(A) : \mathbf{Rel}(m([I]))`$, $`B : \mathbf{Rel}(m([J]))`$, $`A \leq B`$, $`m(I) \leq m(J)`$.

**Case PLAY_PATT**
* `$A plays $I` is satisfied if $`m(A) \leq A' <_! m(I)`$ (for $`A'`$ **not** an interface type)
* `$A plays! $I` is satisfied if $`m(A) <_! m(I)`$

**Case OWNS_PATT**
* `$A owns $B` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type)
* `$A owns! $B` is satisfied if $`m(A) <_! m(O_B)`$ 

_Remark_. In particular, if `A owns B[]` has been declared, then `$X owns B` will match the answer `m($X) = A`.

#### Constraints

_Remark: the usefulness of constraint patterns seems overall low, could think of a different way to retrieve full schema or at least annotations (this would be more useful than, say,having to find cardinalities by "trialing and erroring" through matching). STICKY: discuss!_

**Case CARD_PATT**
* cannot match `@card(n..m)` (STICKY: discuss! `@card($n..$m)`??)
<!-- 
* `A relates I @card(n..m)` is satisfied if $`m(A) : \mathbf{Rel}(m(I))`$ and schema allows $`|a|_I`$ to be any number in range `n..m`.
* `A plays B:I @card(n..m)` is satisfied if ...
* `A owns B @card(n...m)` is satisfied if ...
* `$A relates $I[] @card(n..m)` is satisfied if ...
* `$A owns $B[] @card(n...m)` is satisfied if ...
-->

**Case PLAYS_AS_PATT**

_Notation: for readability, we simply write $`X`$ in place of $`m(X)`$ in this case and the next._

* `$A plays $B:$I as $C:$J` is satisfied if $A \leq A' <_! D' \leq D$ for some $`D`$s, and $I \leq I' <_! J' \leq J$, with $`A^{(')} \leq {I^{(')}}`$, $`D^{(')} \leq {J^{(')}}`$, and schema directly contains the constraint `A' plays B':I' as C':J'` for relation types $B \leq B' \leq_! C' \leq C$.

**Case OWNS_AS_PATT**
* `$A owns $B as $C` is satisfied if $A \leq A' <_! D' \leq D$ for some $`D`$s, and $B \leq B' <_! C' \leq C$, with $`A^{(')} \leq O_{B^{(')}}`$, $`D^{(')} \leq O_{C^{(')}}`$, and schema directly contains the constraint `A' owns B' as C'`.

_Remark: these two are still not a natural constraint, as foreshadowed by a previous remark!_

**Case UNIQUE_PATT**
* `$A owns $B @unique` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @key`.

* `$A owns! $B @unique` is satisfied if $`m(A) <_! m(O_B)`$, and schema directly contains constraint `m($A) owns m($B) @unique`.

**Case KEY_PATT**
* `$A owns $B @key` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @key`.

* `$A owns! $B @key` is satisfied if $`m(A) <_! m(O_B)`$, and schema directly contains constraint `m($A) owns m($B) @key`.

**Case SUBKEY_PATT**
* `$A owns $B @subkey(<LABEL>)` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @subkey(<LABEL>)`.

**Case ABSTRACT_PATT**
* `(type) $B @abstract` is satisfied if schema directly contains `(type) m($B) @abstract`.
* `$A plays $B:$I @abstract` is satisfied if $`m(A) \leq A'`$, $`m(B) : \mathbf{Rel}(m(I))`$, $`m(B) \leq B' : \mathbf{Rel}(m(I))`$ and schema directly contains constraint `A' plays B':m($I) @abstract`.
* `$A owns $B @abstract` is satisfied if $`m(A) \leq A'`$ and schema directly contains one of the constraints
  * `A' owns m($B) @abstract`
  * `A' owns m($B)[]`

* `$A owns $B[] @abstract` is satisfied if $`m(A) \leq A'`$ and schema directly contains constraint `A' owns m($B)[] @abstract`.
* `$B relates $I @abstract` is satisfied if $`B : \mathbf{Rel}(I)`$, $`m(B) \leq B'`$, and schema directly contains constraint `B' relates m($I) @abstract`.
* `$B relates $I[] @abstract` is satisfied if $`B : \mathbf{Rel}([I])`$, $`m(B) \leq B'`$, and schema directly contains constraint `B' relates m($I)[] @abstract`.

**Case VALUES_PATT**
* cannot match `@values/@regex/@range` (STICKY: discuss!)
<!--
* `A owns B @values(v1, v2)` is satisfied if 
* `A owns B @regex(<EXPR>)` is satisfied if 
* `A owns B @range(v1..v2)` is satisfied if 
* `A value B @values(v1, v2)` is satisfied if 
* `A value B @regex(<EXPR>)` is satisfied if 
* `A value B @range(v1..v2)` is satisfied if 
-->

**Case DISTINCT_PATT**
* `A owns B[] @distinct` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B)[] @distinct`.
* `B relates I[] @distinct` is satisfied if $`m(B) : \mathbf{Rel}(m([I]))`$, $`B \leq B'`$ and schema directly contains `B' relates I[] @distinct`.

#### Data

**Case ISA_PATT**
* `$x isa $T` is satisfied if $`m(x) : m(T)`$ for $`m(T) : \mathbf{ERA}`$
* `$x isa! $T` is satisfied if $`m(x) :_! m(T)`$ for $`m(T) : \mathbf{ERA}`$

**Case LINKS_PATT**
* `$x links ($I: $y)` is satisfied if $`m(x) : A(m(y):m(I))`$ for some $`A : \mathbf{Rel}(m(I))`$.
* `$x links ($I[]: $y)` is satisfied if $`m(x) : A(m(y):[m(I)])`$ for some $`A : \mathbf{Rel}([m(I)])`$.
* `$x links ($y)` is equivalent to `$x links ($_: $y)` for anonymous `$_` (See "Syntactic Sugar")

**Case HAS_PATT**
* `$x has $B $y` is satisfied if $`m(y) : m(B)(m(x):O_{m(B)})`$ for some $`m(B) : \mathbf{Att}`$.
* `$x has $B[] $y` is satisfied if $`m(y) : [m(B)](m(x):O_{m(B)})`$ for some $`m(B) : \mathbf{Att}`$.
* `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`

_Remark_. Note that `$x has $B $y` will match the individual list elements of list attributes (e.g. when $`x : A`$ and $`A <_! O_B`$).

**Case IS_PATT**
* `$x is $y` is satisfied if $`m(x) :_! A`$, $`m(y) :_! A`$, $`m(x) = m(y)`$, for $`A : \mathbf{ERA}`$
* `$A is $B` is satisfied if $`A = B`$ for $`A : \mathbf{ERA}`$, $`B : \mathbf{ERA}`$

***System property***

1. In the `is` pattern, left or right variables are **not bound**.

_Remark_: In the `is` pattern we cannot syntactically distinguish whether we are in the "type" or "element" case (it's the only such pattern where tvars and evars can be in the same position!) but this is alleviated by the pattern being non-binding, i.e. we require further statements which bind these variables, which then determines them to be tvars are evars.

#### Expressions

Expression are part of some patterns, which we discuss in this section under the name "expression patterns". First, we briefly touch on the definition of the grammar for expressions itself. 

**Grammar EXPR**

```javascript
BOOL      ::= VAR | bool 
INT       ::= VAR | long | ( INT ) | INT (+|-|*|/|%) INT 
              | (ceil|floor|round)( DBL ) | abs( INT ) | len( T_LIST )
              | (max|min) ( INT ,..., INT )
DBL       ::= VAR | double | ( DBL ) | DBL (+|-|*|/) DBL 
              | (max|min) ( DBL ,..., DBL ) |        // TODO: convert INT to DBL??
STRING    ::= VAR | string | string + string
TIME      ::= VAR | time | TIME (+|-) TIME 
DATETIME  ::= VAR | datetime | DATETIME (+|-) TIME 
T         ::= T_LIST [ INT ] | STRUCT.T_COMPONENT    // "polymorphic" grammar
T_LIST    ::= VAR | [ T ,..., T ] | T_LIST + T_LIST  // includes empty list []
INT_LIST  ::= VAR | INT_LIST | [ INT .. INT ]
VAL_EXPR  ::= T | T_LIST
DESTRUCT  ::= { T_COMPONENT: (VAR|VAR?|DESTRUCT), ... }   // assume unique component labels
STRUCT    ::= VAR | { T_COMPONENT: (VAL_EXPR|STRUCT)), ... }
EXPR      ::= VAL_EXPR | STRUCT
```

***System property***

1. Generally, variables in expressions `<EXPR>` are **never bound**, except ...
2. The exception are **single-variable list indices**, i.e. `$list[$index]`; in this case `$index` is bound. (This makes sense, since `$list` must be bound elsewhere, and then `$index` is bound to range over the length of the list)
3. Struct components are considered to be unordered: i.e., `{ x: $x, y: $y}` is equal to `{ y: $y, x: $x }`.
4. We assume all struct components to be uniquely named in the schema: as such, each component has a unique associated type. (this is why we can use `T_COMPONENT` above).

_Remark_: The exception for 2. is mainly for convenience. Indeed, you could always explicitly bind `$index` with the pattern `$index in [0..len($list)-1];`. See "Case **IN_LIST_PATT**" below.


**Case ASS_PATT**
* `$x = <EXPR>` is satisfied if $`m(x)`$ equals the expression on the right-hand side, evaluated after substituting answer for all its variables.

***System property***

1. _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Assign once, to vars only_. Any variable can be assigned only once within a pattern—importantly, the left hand side _must be_ a variable (replacing it with a concept will throw an error; this implicitly applies to "Match semantics").
3. _Acyclicity_. It must be possibly to determine answers of all variables in `<EXPR>` before answering `$x` — this avoids cyclic assignments (like `$x = $x + $y; $y = $y - $x;`)

**Case DESTRUCT_PATT**
* `DESTRUCT = STRUCT` is satisfied if, after substituting concepts from `m`, the left hand side (up to potentially omitting components whose variables are marked as optional) matched the structure of the right and side, and each variable on the left matches the evaluated expression of the correponding position on the right.

***System property***

1. _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Acyclicity_. Applies as before.

**Case IN_LIST_PATT**
* `$x in $l` is satisfied if $`m(l) : [A]`$ for $`A : \mathbf{Type}`$ and $`m(x) \in m(l)`$
* `$x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar") 

***System property***

1. The right-hand side variable(s) of the pattern are **not bound**. (The left-hand side variable is bound.)

**Case EQ_PATT**
* `<EXPR> == <EXPR>` is satisfied if, after substituting `m`, the left hand expression evaluates exactly to the right hand one.
* `<EXPR> != <EXPR>` is equivalent to `not { $x == $y }` (see "Patterns")

***System property***

1. All variables are bound **not bound**.

**Case COMP_PATT**

The following are all kind of obvious (for `<COMP>` one of `<`,`<=`,`>`,`>=`):

* `<INT> <COMP> <INT>` 
* `<BOOl> <COMP> <BOOL>` (`false`<`true`)
* `<STRING> <COMP> <STRING>` (lexicographic comparison)
* `<DATETIME> <COMP> <DATETIME>` (usual datetime order)
* `<TIME> <COMP> <TIME>` (usual time order)
* `<STRING> contains <STRING>` 
* `<STRING> like <REGEX>` (where `<REGEX>` is a regex string without variables)

***System property***

1. In all the above patterns all variables are **not bound**.


#### Functions

**Case IN_FUN_PATT**
* `$x, $y?, ... in <FUN_CALL>` is satisfied, after substituting concepts, the left hand side is an element of the **function answer set** $`F`$ of evaluated `<FUN_CALL>` on the right (see "Function semantics") meaning that: for some tuple $t \in F$ we have
  * for the $`i`$th variable `$z`, which is non-optional, we have $`m(z) = t_i`$
  * for the $`i`$th variable `$z`, which is marked as optional using `?`, we have either
    * $`m(z) = t_i`$ and $`t_i \neq \emptyset`$
    * $`m(z) = t_i`$ and $`t_i = \emptyset`$

**Case ASS_FUN_PATT**
* `$x, $y?, ... = <FUN_CALL>` is satisfied, after substituting concepts, the left hand side complies with the **function answer tuple** $`t`$ of `<FUN_CALL>` on the right (see "Function semantics") meaning that:
  * for the $`i`$th variable `$z`, which is non-optional, we have $`m(z) = t_i`$
  * for the $`i`$th variable `$z`, which is marked as optional using `?`, we have either
    * $`m(z) = t_i`$ and $`t_i \neq \emptyset`$
    * $`m(z) = t_i`$ and $`t_i = \emptyset`$

_Remark_: variables marked with `?` in function assignments are the first example of **optional variables**. We will meet other pattern yielding optional variables in the following section.


#### Patterns

Now that we have seen how to determine when answers satisfy individual statements, we can extend our discussion of match semantics to composite patterns (patterns of patterns).

**Case AND_PATT**
* An answer satisfies the pattern `<PATT1>; <PATT2>;` that simultaneously satisfies both `<PATT1>` and `<PATT2>`.


**Case OR_PATT**
* An answer for the pattern `{ <PATT1> } or { <PATT2> };` is an answer that satisfies either `<PATT1>` or `<PATT2>`.

_Remark_: this generalize to a chain of $`k`$ `or` clauses.

**Case NOT_PATT**
* An answer satisfying the pattern `not { <PATT> };` is any answer which _cannot_ be completed to a answer satisfying `<PATT>`.

**Case TRY_PATT**
* The pattern `try { <PATT> };` is equivalent to the pattern `{ <PATT> } or { not { <PATT>}; };`.


## Match semantics

A `match` clause comprises a pattern `P`.

* _Input cmaps_: The clause can take as input a stream `{ m }` of concept maps `m`.

* _Output cmaps_: For each `m`: 
  * replace all patterns in `P` with concepts from `m`. 
  * Compute the stream of answert `{ m' }`. 
  * The final output stream will be `{ (m,m') }`.


## Functions semantics

### Function signature, body, operators

**case FUN_SIGN_STREAM**

_Syntax_:
```
fun F ($x: A, $y: B[]) -> { C, D[], E? } :
```
where
* types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_STICKY: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

**case FUN_SIGN_SINGLE**

_Syntax_:
```
fun F ($x: A, $y: B[]) -> C, D[], E? :
```
where
* types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_STICKY: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

**case FUN_BODY**

_Syntax_:
```
match <PATT>
```
* `<PATT>;` can be any pattern as defined in the previous sections. 

**case FUN_OPS**

_Syntax_:
```
<OP>;
...
<OP>;
```

* `<OP>` can be one of:
  * `limit <int>`
  * `offset <int>`
  * `sort $x, $y` (sorts first in `$x`, then in `$y`)
  * `select $x, $y`
* Each `<OP>` stage takes the concept map set from the previous stage and return a concept map set for the 
  * These concept map set operatioins are described in "Operators"
* The final output concept map set of the last operator is called the **body concept map set**

### Stream-return

* `return { $x, $y, ... }`
  * performs a `select` of the listed variables (See "Select")
  * return resulting concept map set

### Single-return

* `return <AGG> , ... , <AGG>;` where `<AGG>` is one of the following **aggregate functions**:
  * `check`:
    * output type `bool`
    * returns `true` if concept map set non-empty
  * `sum($x)`:
    * output type `double` or `int`
    * returns sum of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty sums yield `0.0` or `0`
  * `mean($x)`:
    * output type `double?`
    * returns mean of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty mean return $\emptyset$
  * `median($x)`, 
    * output type `double?` or `int?` (depending on type of `$x`)
    * returns median of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty medians return $\emptyset$
  * `first($x)`
    * `A?` for any `A`
    * returns sum of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * if no `m($x)`is set, return $\emptyset$
  * `count`
    * output type `long`
    * returns count of all answers
  * `count($x)`
    * output type `long`
    * returns count of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
  * `list($x)`
    * output type `[A]`
    * returns list of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
* Each `<AGG>` reduces the concept map `{ m }` passsed to it from the function's body to a single value in the specified way.

### Recursion and recursive semantics

Functions can be called recursively, as long as negation can be stratified:
* The set of all defined functions is divided into groups called "strata" which are ordered
* If a function `F` calls a function `G` if must be a in an equal or higher stratum. Moreover, if `G` appears behind an odd number of `not { ... }` in the body of `F`, then `F` must be in a strictly strictly stratum.

The semantics in this case is computed "stratum by stratum" from lower strata to higher strata. New facts in our type systems ($`t : T`$) are derived in a bottom-up fashion for each stratum separately.


## Insert semantics

### Basics of inserting

An `insert` clause comprises collection of _insert statements_

* _Input cmap_: The clause can take as input a stream `{ m }` of concept maps `m`, in which case 
  * the clause is **executed** for each map `m` in the stream individually

* _Extending input map_: Insert clauses can extend bindings of the input concept map `m` in two ways
  * `$x` is the subject of an `isa` statement in the `insert` clause, in which case $`m(x) =`$ _newly-inserted-concept_ (see "Case **ISA_INS**")
  * `$x` is the subject of an `=` assignment statement in the `insert` clause, in which case $`m(x) =`$ _assigned-value_ (see "Case **ASS_INS**")

* _Execution_: An `insert` clause is executed by executing its statements individually.
  * Not all statement need to execute (see Optionality below)
    * **runnable** statements will be executed
    * **skipped** statements will not be executed
  * The order of execution is arbitrary except for:
    1. We execute all runnable `=` assignments first.
    2. We then execute all runnable `isa` statements.
    3. Finally, we execute remaining runnable statements.
  * Executions of statements will modify the database state by 
    * adding elements
    * refining dependencies
  * (Execution can also affect the state of concept map `m` as mentioned above)
  * Modification are buffered in transaction (see "Transactions")
  * Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `insert` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if
    * they are bound outside the block
    * they are bound by an `isa` or `=` statement in the block
  * If any variable is not block-level bound, the `try` block statements are skipped.
  * If all variables are block-level bound, the `try` block statements are runnable.
  * All variables outside of a `try` block must be bound outside of that try block (in other words, variable in a block bound with `isa` cannot be used outside of the block)

### Insert statements

**Case ISA_INS**
* `$x isa A` adds new $`a :_! A`$ for $`A : \mathbf{ERA}`$ and sets $`m(x) = a`$
* `$x isa $T` adds new $`a :_! m(T)`$ ($T$ must be bound) and sets $`m(x) = a`$

***System property***:

1. `$x` cannot be bound elsewhere (i.e. `$x` cannot be bound in the input map `m` nor in other `isa` or `=` statements).

**Case ISA_INS**
* `$x = <EXPR>` adds nothing, and sets $`m(x) = v`$ where $`v`$ is the value that `<EXPR>` evaluates to.

***System property***:

1. `$x` cannot be bound elsewhere.
2. All variables in `<EXPR>` must be bound elsewhere (as before, we require acyclicity of assignement, see "Acyclicity").
3. `<EXPR>` cannot contain function calls.

**Case LINKS_INS** 
* `$x links ($I: $y)` refines $`x :_! A(a : J, b : K, ...)`$ to $`x :_! A(m(y)a : m(I), b : K, ...)`$ 

**Case LINKS_LIST_INS** 
* `$x links ($I[]: <T_LIST>)` refines $`x :_! A()`$ to $`x :_! A(l : [m(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

***System property***:

1. Transaction should fail if $`x :_! A(...)`$ already has a roleplayer list. (Need "Update" instead!)

**Case HAS_INS**
* `$x has $A $y` adds new $`m(y) :_! m(A)(m(x) : O_{m(A)})`$

**Case HAS_LIST_INS**
* `$x has $A[] <T_LIST>` adds $`l :_! [m(A)](m(x) : O_{m(A)})`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$
  * _Note_ usage of direct typing implies (non-direct) typings $`l_i : m(A)(m(x) : O_{m(A)})`$

***System property***:

1. Transaction should fail if $`[m(A)](m(x) : O_{m(A)})`$ already has an attribute list. (Need "Update" instead!)


### Optional inserts

**Case TRY_INS**
* `try { <INS>; ...; <INS>; }` where `<INS>` are insert statements as described above.
  * `<TRY_INS>` blocks can appear alongside other insert statements in an `insert` clause
  * Execution is as described in "Basics of inserting"

### Leaf attribute system constraint

***System property***:

1. Cannot add $`m(y) :_! A(m(x) : O_A)`$ if there exists $`B \leq A`$.

_Remark_. We want to get rid of this constraint (STICKY).



## Delete semantics

### Basics of deleting


A `delete` clause comprises collection of _delete statements_.

* _Input cmaps_: The clause can take as input a stream `{ m }` of concept maps `m`: 
  * the clause is **executed** for each map `m` in the stream individually

* _Updating input maps_: Delete clauses can update bindings of their input concept map `m`
  * Executing `delete $x;` will remove `$x` from `m` (but `$x` may still appear in other cmaps `m'` of the input stream)

_Remark_: Previously, it was suggested: if `$x` is in `m` and $`m(x)`$ is deleted from $`T_m(x)`$ by the end of the execution of the clause (for _all_ input maps of the input stream) then we set $`m(x) = \emptyset`$ and $`T_m(x) = \emptyset`$.
Fundamental question: **is it better to silently remove vars? Or throw an error if vars pointing to deleted concepts are used?** (STICKY)
* Only for `delete $x;` can we statically say that `$x` must not be re-used
* Other question: would this interact with try? idea: take $`m(x) = \emptyset`$ if it points to a previously deleted concept

<!--
match 
  $x isa Thing, has Property $p;
delete 
  Property $p of $x;
... // can still use $p

match
  $x isa Thing, has Id "id", has Property $p;
  $y isa Bool; try { $y == true; $x has Property $q; }
delete
  try { Property $q of $x; }
... // output could be:
($x -> "id", $p -> EMPTY, $y -> false)
($x -> "id", $p -> EMPTY, $y -> true, $q -> EMPTY)

... removing attributes tells us NOTHING about how many "has" references were actually deleted!!
-->

* _Execution_: An `delete` clause is executed by executing its statements individually.
  * Not all statement need to execute (see Optionality below)
    * **runnable** statements will be executed
    * **skipped** statements will not be executed
  * The order of execution is arbitrary order.
  * Executions of statements will modify the database state by 
    * removing elements 
    * coarsening dependencies
  * Modification are buffered in transaction (see "Transactions")
  * Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if they are bound in `m`
  * If any variable is not block-level bound, the `try` block statements are **skipped**.
  * If all variables are block-level bound, the `try` block statements are **runnable**.

### Delete statements

**case CONCEPT_DEL**
* `$x;` removes $`m(x) :_! A(...)`$. If $`m(x)`$ is an object, we also:
  * coarsen $`b :_! B(m(x) : I, z : J, ...)`$ to $`b :_! B(z : J, ...)`$ for any such dependency on $`m(x)`$

_Remark 1_. This applies both to $`B : \mathbf{Rel}`$ and $`B : \mathbf{Att}`$.

_Remark 2_. The resulting $`m(x) :_! m(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

***System property***:

1. If $`m(x) : A : \mathbf{Att}`$ and $`A`$ is _non_ marked `@independent` then the transaction will fail.


**case CONCEPT_CASC_DEL**
* `$x @cascade(C, D, ...)` removes $`m(x) :_! A(...)`$. If $`m(x)`$ is an object, we also:
  * coarsen $`b :_! B(m(x) : I, z : J, ...)`$ to $`b :_! B(z : J, ...)`$. Next:
    * if the following are _both_ satisfied:
      1. the coarsened axiom $`b :_! B(...)`$ violates interface cardinality of $`B`$,
      2. $`B`$ is among the listed types `C, D, ...`

      then: **recursively execute** delete statement `b isa B @cascade(C, D, ...)`

_Remark_. In an earlier version of the spec, condition (1.) for the recursive delete was omitted—however, there are two good reasons to include it:

1. The extra condition only makes a difference when non-default interface cardinalities are imposed, in which case it is arguably useful to adhere to those custom constraints.
2. The extra condition ensure that deletes cannot interfere with one another, i.e. the order of deletion does not matter.

**case ROL_OF_DEL**
* `($I: $y) of $x` coarsens $`m(x) :_! m(A)(m(y) : m(I), z : J, ...)`$ to $`m(x) :_! m(A)(z : J, ...)`$

_Remark_. The resulting $`m(x) :_! m(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

**case ROL_LIST_OF_DEL**
* `($I[]: <T_LIST>) of $x` coarsens $`m(x) :_! m(A)(l : m(I))`$ to $`m(x) :_! m(A)()`$ for $`l`$ being the evaluation of `T_LIST`.

**case ATT_OF_DEL**
* `$B $y of $x` coarsens $`m(y) :_! B'(m(x) : O_{m(B)})`$ to $`m(y) :_! B'()`$ for all possible $`B' \leq m(B)`$

_Remark_. Note the subtyping here! It only makes sense in this case though since the same value `$y` may have been inserted in multiple attribute subtypes (this is not the case for **LINKS_DEL**)—at least if we lift the "Leaf attribute system constraint".

**case ATT_LIST_OF_DEL**
* `$B[] <T_LIST> of $x` deletes $`l :_! B'(m(x) : O_{m(B)})`$ for all possible $`B' \leq m(B)`$ and $`l`$ being the evaluation of `T_LIST`. (STICKY: discuss! Suggestion: we do not retain list elements as independent attributes.)


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Update semantics

### Basics of updating

A `update` clause comprises collection of _update statements_.

* _Input cmap_: The clause can take as input a stream `{ m }` of concept maps `m`, in which case 
  * the clause is **executed** for each map `m` in the stream individually

* _Updating input maps_: Update clauses do not update bindings of their input cmap `m`

* _Execution_: An `update` clause is executed by executing its statements individually in any order.
  * STICKY: this might be non-deterministic if the same thing is updated multiple times, solution outlined here: throw error if that's the case!

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if they are supplied by `m`
  * If any variable is not block-level bound, the `try` block statements are **skipped**.
  * If all variables are block-level bound, the `try` block statements are **runnable**.

### Update statements

**case LINKS_UP**
* `$x links ($I: $y);` updates $`m(x) :_! A(b:J)`$ to $`m(x) :_! A(m(x) : m(I))`$

***System property***:

1. Require there to be exactly one present roleplayer for update to succeed.
1. Require that each update happens at most once, or fail the transaction. (STICKY: discuss!)

**Case LINKS_LIST_UP** 
* `$x links ($I[]: <T_LIST>)` updates $`x :_! A(j : [m(I)])`$ to $`x :_! A(l : [m(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

***System property***:

1. Require there to be a present roleplayer list for update to succeed (can have at most one).
1. Require that each update happens at most once, or fail the transaction.

**case HAS_UP**
* `$x has $B: $y;` updates $`b :_! m(B)(x:O_{m(B)})`$ to $`m(y) :_! m(B)(x:O_{m(B)})`$

***System property***:

1. Require there to be exactly one present attribute for update to succeed.
1. Require that each update happens at most once, or fail the transaction.

**Case HAS_LIST_UP**
* `$x has $A[] <T_LIST>` updates $`j :_! [m(A)](m(x) : O_{m(A)})`$ to $`l :_! [m(A)](m(x) : O_{m(A)})`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

***System property***:

1. Require there to be a present attribute list for update to succeed.
1. Require that each update happens at most once, or fail the transaction.


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Put semantics

`put <PUT>` is equivalent to 
```
if (match <PUT>; check;) then (match <PUT>;) else (insert <PUT>)
```
In particular, `<PUT>` needs to be an `insert` compatible set of statements. 

# System execution

## Pipelines 

Pipelines comprises chains of clauses and operators.

_Key principle_:

* Clauses and operators are executed eagerly
* In this way, executing later stages of the pipeline can never affect earlier stages.

### Basics of clauses

Clauses are stages in which patterns are matched or statements are executed.

#### Match

As described in "Match semantics".

#### Insert

As described in "Insert semantics".

#### Delete

As described in "Delete semantics".

#### Update

As described in "Update semantics".

#### Put

As described in "Put semantics".

#### Fetch

The `fetch` clause is of the form

```
fetch { 
 <fetch-KV-statement>;
 ...
 <fetch-KV-statement>;
}
```

* The `fetch` clause takes as input a cmap stream `{ m }`
* It output a stream `{ doc<m> }` of JSON documents (one for each `m` in the input stream)
* The `fetch` clause is **terminal**

**case FETCH_VAL**
* `"key": $x`

**case FETCH_EXPR**
* `"key": <EXPR>`

_Note_. `<EXPR>` can, in particuar, be `T_LIST` expression (see "Expressions").

**case FETCH_ATTR**
* `"key": $x.A` where $`A : \mathbf{Att}`$

***System property***

1. fails transaction if $`T_m(x)`$ does not own $`A`$.
1. fails transaction if $`T_m(x)`$ does not own $`A`$ with `card(1,1)`.

**case FETCH_MULTI_ATTR**
* `"key": [ $x.A ]` where $`A : \mathbf{Att}`$

***System property***

1. fails transaction if $`T_m(x)`$ does not own $`A`$.

**case FETCH_LIST_ATTR**
* `"key": $x.A[]` where  $`A : \mathbf{Att}`$

***System property***

1. fails transaction if $`T_m(x)`$ does not own $`[A]`$.

**case FETCH_SNGL_FUN**
* `"key": fun(...)` where `fun` is single-return.

**case FETCH_STREAM_FUN**
* `"key": [ fun(...) ]` where `fun` is stream-return.

_Note_: (STICKY:) what to do if type inference for function args fails based on previous pipeline stages?

**case FETCH_FETCH**
```
"key": [ 
  match <PATTERN>;
  fetch { <FETCH> }
]
```

**case FETCH_REDUCE** 
```
"key": [ 
  match <PATTERN>;
  reduce <AGG>, ... , <AGG>; 
]
```

**case FETCH_NESTED**
```
"key" : { 
  <fetch-KV-statement>;
  ...
  <fetch-KV-statement>;
}
```

### Basics of operators

Operators (unlike clauses) do not comprise patterns are statements—they describe direct operations on streams.

#### Select

`select $x, $y` 
transforms


#### Deselect 

#### Sort

#### Limit

#### Offset

_Remark_: Offset is only useful when streams (and the order of answers) are fully deterministic.

#### Reduce

* The `reduce` operator takes as input a stream of maps `{ m }`
* It outputs a tuple of values
* `reduce` operator is **terminal** (i.e. terminates the pipeline)

```
reduce <AGG> , ... , <AGG>;
``` 

* `<AGG>` is one of the following **aggregate functions**:
  * `check`:
    * output type `bool`
    * outputs `true` if concept map set non-empty
  * `sum($x)`:
    * output type `double` or `int`
    * outputs sum of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty sums yield `0.0` or `0`
  * `mean($x)`:
    * output type `double?`
    * outputs mean of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty mean output $\emptyset$
  * `median($x)`, 
    * output type `double?` or `int?` (depending on type of `$x`)
    * outputs median of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * empty medians output $\emptyset$
  * `first($x)`
    * `A?` for any `A`
    * outputs sum of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
    * if no `m($x)`is set, outputs $\emptyset$
  * `count`
    * output type `long`
    * outputs count of all answers
  * `count($x)`
    * output type `long`
    * outputs count of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
  * `list($x)`
    * output type `[A]`
    * returns list of all non-empty `m($x)` in concept map `m`
    * `$x` can be optional
* Each `<AGG>` reduces the concept map `{ m }` passsed to it from the function's body to a single value in the specified way.


## Transactions

(to be written)

### Basics

### Snapshots

### Concurrency

# Sharding

(to be written)

# Glossary

## Type system

### Type 

Any type in the type system.

### Schema type

A type containg data inserted into the database. Cases: 

* Entity type, 
* Relation type, 
* Attribute type,
* Interface type.

### Value type

A type containing a pre-defined set of values. Cases:

* Primitive value type: `bool`, `string`, ...
* Structured value type / struct value type / struct: user-defined

### Data instance / instance

An element in an entity, relation, or attribute type. Special cases:

* Entity: An element in an entity type.
* Relation: An element in a relation type.
* Attribute: An element in attribute type.
* Object: An element in an entity or relation type.
* Roleplayer: Object that is cast into the element of a role type.
* Owner: Object that is cast into the element of an ownership type.

### Data value / value

An element in a value type.

### Attribute instance value / attribute value

An attribute cast into an element of its value type.

### Data element / element

Any element in any type (i.e. value or instance).

### Concept

A element or a type.

### Concept map

Mapping of variables to concepts

### Stream

An ordered concept map.

### Answer set

The set of concept maps that satisfy a pattern in the minimal way.

### Answer 

An element in the answer set of a pattern.


## TypeQL syntax

### Schema query

(Loosely:) Query pertaining to schema manipulation

### Data query

(Loosely:) Query pertaining to data manipulation or retrieval

### Clause / Stream clause

* `match`, `insert`, `delete`, `define`, `undefine`,

### Operators / Stream operator

* `select`, `sort`, ... 

### Functions

callable `match-return` query. can be single-return or stream-return.

### Statement

A syntactic unit (cannot be subdivided). Variations:

* Simple statement: statement not containing `,`
* Combined statement: statement combined with `,`

### Pattern

Context for variables (this "context" describes properties of the variables), used to form a data retrieval query.

### Stream reduction / reduction

* `list`, `sum`, `count` ...

### Clause

part of a query pipeline

### Block

`{ pattern }` 

### Suffix

`[]` and `?`.


## Syntactic Sugar

* `$x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar") 
* `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`
* `$x links ($y)` is equivalent to `$x links ($_: $y)` for anonymous `$_` (See "Syntactic Sugar")
* `$x has $B <EXPR>` - `$x has $B $y; $y = <EXPR>`
* `$x has $B <COMP>` - `$x has $B $y; $y <COMP>`
* `$x has $y` - `$x has $_Y $y;` (_not_: `$_Y[]` !)

## Typing of operators

```
+ : T_LIST x T_LIST -> T_LIST
+ : STRING x STRING -> STRING
+ : DBL x DBL -> DBL
```

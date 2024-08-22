# TypeDB - Behaviour Specification

<!-- TODO 
* \gamma -> \mathsf{ans}
* concept(..) -> ans(...)
* \tau -> \mathsf{typ}
-->

**Table of contents**

- [Foundations](#foundations)
  - [Terminology](#terminology)
  - [The type system](#the-type-system)
- [Schema](#schema)
  - [Basics of schemas](#basics-of-schemas)
  - [Define semantics](#define-semantics)
    - [Types, subtypes, dependencies](#types-subtypes-dependencies)
    - [Constraints](#constraints)
    - [Triggers](#triggers)
    - [Value types](#value-types)
    - [Functions defs](#functions-defs)
  - [Undefine semantics](#undefine-semantics)
    - [Types, subtypes, dependencies](#types-subtypes-dependencies-1)
    - [Constraints](#constraints-1)
    - [Triggers](#triggers-1)
    - [Value types](#value-types-1)
    - [Functions defs](#functions-defs-1)
  - [Redefine semantics](#redefine-semantics)
    - [Types, subtypes, dependencies](#types-subtypes-dependencies-2)
    - [Constraints](#constraints-2)
    - [Triggers](#triggers-2)
    - [Value types](#value-types-2)
    - [Functions defs](#functions-defs-2)
- [Data instance languages](#data-instance-languages)
  - [Match semantics](#match-semantics)
    - [Basics: Variables and Answers](#basics-variables-and-answers)
    - [Satisfying type patterns](#satisfying-type-patterns)
    - [Satisfying constraint patterns](#satisfying-constraint-patterns)
    - [Satisfying data patterns](#satisfying-data-patterns)
    - [Satisfying value expression patterns](#satisfying-value-expression-patterns)
    - [Satisfying function patterns](#satisfying-function-patterns)
    - [Satisfying composite patterns](#satisfying-composite-patterns)
  - [Function semantics](#function-semantics)
    - [Function body and operators](#function-body-and-operators)
    - [Stream-return](#stream-return)
    - [Single-return](#single-return)
    - [Recursion](#recursion)
  - [Insert semantics](#insert-semantics)
    - [Leaf attribute system constraint](#leaf-attribute-system-constraint)
  - [Delete semantics](#delete-semantics)
  - [Update semantics](#update-semantics)
  - [Put semantics](#put-semantics)
- [Pipelines](#pipelines)
  - [Basics of streams](#basics-of-streams)
  - [Clauses](#clauses)
    - [Match](#match)
    - [Insert](#insert)
    - [Delete](#delete)
    - [Update](#update)
    - [Fetch](#fetch)
  - [Operators](#operators)
    - [Select](#select)
    - [Sort](#sort)
    - [Limit](#limit)
    - [Offset](#offset)
    - [Reduce](#reduce)
  - [Execution](#execution)
- [Transactionality and Concurrency](#transactionality-and-concurrency)
  - [Basics](#basics)
  - [Snapshots](#snapshots)
  - [Isolation](#isolation)
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
    - [Type kind](#type-kind)
  - [TypeQL syntax](#typeql-syntax)
    - [Schema query](#schema-query)
    - [Data query](#data-query)
    - [Clause / Stream clause](#clause--stream-clause)
    - [Operators / Stream operator](#operators--stream-operator)
    - [Functions](#functions)
    - [Statement](#statement)
    - [Stream reduction / reduction](#stream-reduction--reduction)
    - [Clause](#clause)
    - [Block](#block)
    - [Suffix](#suffix)


# Foundations

_(For reference only)_

## Terminology

* **TT** — transaction time
  * _Interpretation_: any time within transaction
* **CT** — commit time
  * _Interpretation_: time of committing a transaction to DB
* **tvar** — type variable
* **evar** - data variable

_Remark_: **tvar**s and **evar**s are uniquely distinguish everywhere in TypeQL

## The type system

* **Types**. We write 
  $`A : \mathbf{Type}`$ to mean:
  > $A$ is a type. 

  * _Variations_: in general, may replace $\textbf{Type}$ by: 
    * $\mathbf{Ent}$ (collection of entity types)
    * $\mathbf{Rel}$ (collection of relation types)
    * $`\mathbf{Att}`$ (collection of attribute types)
    * $`\mathbf{Itf}`$ (collection of interface types)
  * _Useful abbreviations_:
    * $\mathbf{Obj} = \mathbf{Ent} + \mathbf{Rel}$ (collection of object types)
    * $`\mathbf{ERA} = \mathbf{Ent} + \mathbf{Rel} + \mathbf{Att}`$ (collection of ERA types)
  * _Example_: $`\mathsf{Person} : \mathbf{Ent}`$ means $`\mathsf{Person}`$ an entity type.
* **Typing**
  If $A$ is a type, then we may write $`a : A`$ to mean:
  > $a$ is an element in type $A$.

  * _Direct typing_: We write $a :_! A$ to mean:
    > $a$ was declared as an element of $A$ by the user (we speak of a ***direct typing***).

  * _Example_: $p : \mathsf{Person}$ means $p$ is of type $\mathsf{Person}$
* **Dependent types**. We write $`A : \mathbf{Type}(I,J,...)`$ to mean:
  > $A$ is a type with interface types $`I, J, ...`$.
  
  * _Application_: Writing $A : \mathbf{Type}(I,J,...)$ ***implies*** $`A(x:I, y:J, ...) : \mathbf{Type}`$ whenever we have $x: I, y: J, ...$.
  * _Variations_: We may replace $`\mathbf{Type}`$ by $`\mathbf{Rel}`$ or $`\mathbf{Att}`$.
  * _Example_: $`\mathsf{Marriage : \mathbf{Rel}(Spouse)}`$ is a relation type with interface type $`\mathsf{Spouse} : \mathbf{Itf}`$.
* **Dependent typing**.  We write $`a : A(x : I, y : J,...)`$ to mean:
  > The element $a$ lives in the type "$`A`$ of $`x`$ (cast as $`I`$), and $`y`$ (cast as $`J`$), and ...".

  * _Notation for grouping interfaces_: We write $`a : A(x : I, y : I)`$ as $`A : A(\{x,y\}:I^2)`$. (Similarly, when $I$ appears $k$ times in $`A(...)`$, write $I^k$)
  * _Role cardinality_: $|a|_I$ counts elements in $\{x_1,...,x_k\} :I^k$
  * **Example**: $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$. Then $|m|_{\mathsf{Spouse}} = 2$.
* **Key properties of dependencies**. (These are some key rules of the type system!)
  * _Combining dependencies_: Given $A : \mathbf{Type}(I)$ and $A : \mathbf{Type}(J)$, this ***implies*** $A : \mathbf{Type}(I,J)$. In words:
    > If a type separately depends on $I$ and on $J$, then it may jointly depend on $I$ and $J$! 

    * _Remark_: This applies recursively to types with $k$ interfaces.
    * _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$
  * _Weakening dependencies_: Given $A : \mathbf{Type}(I,J)$, this ***implies*** $A : \mathbf{Type}(I)$. In words:
    > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

    * _Remark_: This applies recursively to types with $k$ interfaces.
    * _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse^2})`$ implies $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and also $`\mathsf{Marriage} : \mathbf{Rel}`$ (we identify the empty brackets "$`()`$" with no brackets).
  * _Inheriting dependencies_: If $A : \mathbf{Type}$, $B : \mathbf{Type}(I)$, $A < B$ and _not_ $A : \mathbf{Type}(J)$ with $J < I$, then $A : \mathbf{Type}(I)$. In words:
    > Dependencies that are not overwritten are inherited

* **Casting**. We write $`A < B`$ to mean:
  > type casts from $A$ to $B$ are possible: 
  
  * _Casting rule_: If $`A < B`$ and $a : A$, then this ***implies*** $a : B$.
  * _Transitivity rule_: If $`A < B`$ and $B < C$, then this ***implies*** $A < C$.
  * _Reflexivity rule_: If $`A : \mathbf{Type}`$ then this **implies** $A < A$ (notation: we sometimes write $`A \leq B`$ to put extra emphasis on the case $`A = B`$ being possible ... but this is also the case for $`A < B`$.)

  * _Direct castings_: We write $`A <_! B`$ to mean:
    > A cast from A to B was declared by user (we spak of a ***direct casting*** from A to B).

    * _Direct-to-general rule_: $`A <_! B`$ ***implies*** $`A < B`$.
    * _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$
    * _Example_: $`\mathsf{Child} <_! \mathsf{Nameowner}`$
    * _Example_: $`\mathsf{Person} <_! \mathsf{Spouse}`$
  * _"Weakening dependencies" casting_: If $`a : A(x:I, y:J)`$ then $`a : A(x:I)`$. In other words:
    > Elements in $`A(I,J)`$ casts into elements of $`A(I)`$.

    * _Remark_: This applies recursively for types with $k$ interfaces.
    * _Remark 2_: This casting preserves direct typings! I.e. when $`a :_! A(x:I, y:J)`$ then $`a :_! A(x:I)`$
    * _Example_: If $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$ then both $m : \mathsf{Marriage}(x:\mathsf{Spouse})$ and $m : \mathsf{Marriage}(y:\mathsf{Spouse})$
  * _"Covariance of dependencies" casting_: Given $`A < B`$, $`I < J`$ such that $`A : \mathbf{Type}(I)`$ $`B : \mathbf{Type}(J)`$, then $`a : A(x:I)`$ implies $`a : B(x:J)`$. In other words:
    > When $A$ casts to $B$, and $I$ to $J$, then $`A(I)`$ casts to $`B(J)`$.

    * _Remark_: This applies recursively for types with $k$ interfaces.
    * _Example_: If $m : \mathsf{HeteroMarriage}(x:\mathsf{Husband}, y:\mathsf{Wife})$ then $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$
* **List types**. We write $`[A] : \mathbf{Type}`$ to mean
  > the type of $A$-lists, i.e. the type which contains lists $`[a_0, a_1, ...]`$ of elements $`a_i : A`$.

  * _Dependency on list types_: We allow $`A : \mathbf{Type}([I])`$, and thus our type system has types $`A(x:[I]) : \mathbf{Type}`$.
    > $`A(x:[I])`$ is a type depending on lists $`x : [I]`$.

    * _Example_: $`\mathsf{FlightPath} : \mathbf{Rel}([\mathsf{Flight}])`$
  * _Dependent list types_: We allow $`[A] : \mathbf{Type}(I)`$, and thus our type system has types $`[A](x:I) : \mathbf{Type}`$.
    > $`[A](x:I)`$ is a type of $A$-lists depending on interface $I$.

    * _Dependent list type rule_: We ***postulate*** $`[A](x:I) < [A]`$. This reflects that:
       > Every element $l$ of $`[A](x:I)`$ is actually an $A$-list $`l : [A]`$.

    * _Example_: $`[a,b,c] : [\mathsf{MiddleName}](x : \mathsf{MiddleNameListOwner})`$
  * _List length_: for list $l : [A]$ the term $\mathrm{len}(l) : \mathbb{N}$ represents $l$'s length
  * _Abstractness_: all list types are abstract by default, i.e. their terms cannot be explicitly declared (a la $`l :_! [A](x:I)`$)
* **Sum types**. $`A + B`$ — Sum type
* **Product types**. $`A \times B`$ — Product type
* **Type cardinality**.$`|A| : \mathbb{N}`$ — Cardinality of $A$

_Remark for nerds: list types are neither sums, nor products, nor polynomials ... they are so-called _inductive_ types!_


# Schema

This section describes valid declarations of _types_ and axioms relating types (_dependencies and _type castings_) for the user's data model, as well as _model constraints_ that can be further imposed. These declarations are subject to a set of _type system properties_ as listed in this section. The section also describes how such declarations can be manipulated after being first declared (undefine, redefine).

## Basics of schemas

* Kinds of definition clauses:
  * `define`: adds type axioms or model constraints
  * `undefine`: removes type axioms or model constraints
  * `redefine`: both removes and adds axioms or model constraints (remove is never no-op)
* Loose categories for the main schema components:
  * **Types, subtypes, and dependencies**: the "core" of the user model.
  * **Constraints**: postulated conditions that the model satisfies.
  * **Triggers**: actions to be executed based on data/model changes.
  * **Value types**: types for primitive and structured values.
  * **Functions**: parametrized query templates/pre-defined "model logic"
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

### Types, subtypes, dependencies

**Case ENT**
* `entity A` adds $`A : \mathbf{Ent}`$
* `(entity) A sub B` adds $`A : \mathbf{Ent}, A <_! B`$

***System property***: 

1. _Single inheritance_: Cannot have $A <_! B`$ and $A <_! C \neq B$

**Case REL**
* `relation A` adds $`A : \mathbf{Rel}`$
* `(relation) A sub B` adds $`A : \mathbf{Rel}, A <_! B`$, ***requiring*** that $`B : \mathbf{Rel}`$ 
* `(relation) A relates I` adds $`A : \mathbf{Rel}(I)$
* `(relation) A relates I as J` adds $`A : \mathbf{Rel}(I)`$, $`I <_! J`$, ***requiring*** that $`B : \mathbf{Rel}(J)`$ and $A <_! B$
* `(relation) A relates I[]` adds $`A : \mathbf{Rel}([I])$
* `(relation) A relates I[] as J[]` adds $`A : \mathbf{Rel}([I])`$, $`I <_! J`$, ***requiring*** that $`B : \mathbf{Rel}([J])`$ and $A <_! B$

***System property***: 

1. _Single inheritance_: Cannot have $`A <_! B`$ and $A <_! C \neq B$
2. _Single inheritance (for interfaces)_: Cannot have $`I <_! J`$ and $I <_! K \neq J$
3. _Exclusive interface modes_: Cannot have both $`A : \mathbf{Rel}(I)`$ and $`A : \mathbf{Rel}([I])`$ (in other words, cannot have both `A relates I` and `A relates I[]`).
4. _Implicit inheritance_: Cannot redeclare inherited interface (i.e. when `B relates I`, `A sub B` we cannot re-declare `A relates I`... this is automatically inherited!)

**Case ATT**
* `attribute A` adds $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$ ($O_A$ being automatically generated ownership interface)
* `(attribute) A value V` adds $`A < V`$, ***requiring*** that $V$ is a primitive or struct value type
* `(attribute) A sub B` adds $`A : \mathbf{Att}(O_A)`$, $`A <_! B`$ and $`O_A <_! O_B`$, ***requiring*** that $`B : \mathbf{Att}(O_A)`$

***System property***: 

1. _Single inheritance_: Cannot have $A <_! B`$ and $A <_! C \neq B$

**Case PLAYS**

* `A plays B:I` adds $`A <_! I`$, ***requiring*** that $B: \mathbf{Rel}(I)$, $`A :\mathbf{Obj}`$ and not $B \lneq B'$ with $B': \mathbf{Rel}(I)$

_Remark_. The last part of the condition ensure that we can only declare `A plays B:I` if `I` is a role directly declared for `B`, and not an inherited role.

**Case OWNS**
* `A owns B` adds $`A <_! O_B`$, ***requiring*** that $B: \mathbf{Att}(O_B)$, $`A :\mathbf{Obj}`$
* `A owns B[]` adds $`A <_! O_B`$, ***requiring*** that $B: \mathbf{Att}(O_B)$, **puts B[] to be non-abstract**: i.e. allows declaring terms $`l :_! [B](x:O_B)`$, see earlier discussion of list types

_Remark: based on recent discussion, `A owns B[]` _implies_ `A owns B @abstract` (abstractness is crucial here, see `abstract` constraint below). See also the remark in "Satisfying type patterns"._

***System property***: 

1. _Exclusive interface modes_: Only one of `A owns B` or `A owns B[]` can be declared in the model.
2. _Consistent interface modes_: If `A owns B`, and $A' < A$, $B' < B$, then disallow declaring `A' owns B'[]`.
3. _Consistent interface modes (list case)_: If `A owns B[]`, and $A' < A$, $B' < B$, then disallow declaring `A' owns B'`.

### Constraints

**Case CARD**
* `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $`a : A'(\{...\} : I^k)`$, $`A' \leq A`$, $`A' : \mathbf{Rel}(I)`$, and $k$ is _maximal_ (for fixed $a : A$).
  * **defaults** to `@card(1..1)` if omitted ("one")
* `A plays B:I @card(n..m)` postulates $n \leq |B(a:I)| \leq m$ for all $a : A$
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B @card(n...m)` postulates $n \leq |B(a:I)| \leq m$ for all $a : A$
  * **defaults** to `@card(0..1)` if omitted ("one or null")

***System property***:

1. For inherited interfaces, we cannot redeclare cardinality (this is actually a consequence of "Implicit inheritance" above). 
2. When we have direct subinterfaces $I_i <_! J$, for $i = 1,...,n$, and each $I_i$ has `card(`$`n_i,m_i`$`)` while J has $card(n,m)$ then we must have $`n \leq \sum_i n_i \leq \sum_i m_i \leq m`$.
  
_Remark 1: Upper bounds can be omitted, writing `@card(2..)`, to allow for arbitrary large cardinalities_

_Remark 2: For cardinality, and for most other constraints, we should reject redundant conditions, such as `A owns B card(0..3);` when `A sub A'` and `A' owns B card(1..2);`_

**Case CARD_LIST**
* `A relates I[] @card(n..m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $a : A'(l : [I])$, $A' \leq A$, $A' : \mathbf{Rel}([I])$, and $k$ is _maximal_ (for fixed $a : A$).
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B[] @card(n...m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`l : [B](a:O_B)`$ for $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")

**Case PLAYS_AS**
* `A plays B as C` postulates $`|C(x:O_C)| - |B(x:O_B)| = 0`$ for all $`x:A`$, ***requiring*** that $B \lneq C$, $A < D$, $`D <_! C`$. **Invalidated** when $A <_! C'$ for $B \lneq C' \leq C$.

**Case OWNS_AS**
* `A owns B as C` postulates $`|C(x:O_C)| - |B(x:O_B)| = 0`$ for all $`x:A`$, ***requiring*** that $B \lneq C$, $A < D$, $`D <_! O_C`$. **Invalidated** when $A <_! O_{C'}$ for $B \lneq C' \leq C$.

_Comment: both preceding cases are kinda complicated/unnatural ... as reflected by the math._

**Case UNIQUE**
* `A owns B @unique` postulates that if $`b : B(a:O_B)`$ for some $a : A$ then this $a$ is unique (for fixed $b$).

**Case KEY**
* `A owns B @key` postulates that if $`b : B(a:O_B)`$ for some $a : A$ then this $a$ is unique, and also $|B(a:O_B) = 1$.

**Case SUBKEY**
* `A owns B1 @subkey(<LABEL>); A owns B2 @subkey(<LABEL>)` postulates that if $b : B_1(a:O_{B_1}) \times B_2(a:O_{B_2})`$ for some $a : A$ then this $a$ is unique, and also $|B_1(a:O_{B_1}) \times B_2(a:O_{B_2})| = 1$. **Generalizes** to $n$ subkeys.

**Case ABSTRACT**
* `(type) A @abstract` postulates $`a :_! A(...)`$ to be impossible
* `B relates I @abstract` postulates $`A <_! I`$ to be impossible for $A : \mathbf{Obj}$
* `B relates I[] @abstract` postulates $`A <_! I`$ to be impossible for $A : \mathbf{Obj}$
* `A plays B:I @abstract` postulates that
  *  (if $I$ is used as a plain role:) $`b :_! B'(a:I)`$ 
  *  (if $I$ is used as a list role:) $`b :_! B'(l:[I])`$, $a \in l$ 
  
  is impossible whenever $a : A$, $B' \leq B$ (_note_: $B' < B$ is needed here, since the interface $I$ may be inherited to some subtypes)
* `A owns B @abstract` postulates $`b :_! B(a:I)`$ to be impossible for $a : A$ 
* `A owns B[] @abstract` postulates $`b :_! [B](a:I)`$ to be impossible for $a : A$ 

***System property***:

> _The following properties capture that parents of abstract things are meant to be abstract too. But this is not really a crucial condition. (TODO: discuss!)_ 

_Notation_: Write $X(I) < Y(J)$ to mean $X < Y$, $I < J$, $X : \mathbf{Type}(I)$, $Y : \mathbf{Type}(J)$.

1. If `(type) A @abstract` and $A < B$ then `(type) B` cannot be non-abstract.
2. If `A relates I @abstract` and $A(I) < B(J)$ then `B relates J` cannot be non-abstract.
3. If `A relates I[] @abstract` and $A([I]) < B([J])$ then `B relates J[]` cannot be non-abstract.
4. If `A plays B:I @abstract` and $B(I) < C(J)$ then `A plays B:J` cannot be non-abstract.
5. If `A owns B @abstract` and $B < C$ then `A owns C` cannot be non-abstract. 
6. If `A owns B[] @abstract` and $B < C$ then `A owns C[]` cannot be non-abstract. 

**Case VALUES**
* `A owns B @values(v1, v2)` postulates if $a : A$ then $`a \in \{v_1, v_2\}`$ , ***requiring*** that 
  * either $`A : \mathbf{Att}`$, $`A < V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs. 
  
  **Generalizes** to $n$ values.
* `A owns B @regex(v1..v2)` postulates if $a : A$ then $`a`$ conforms with regex `<EXPR>`.
* `A owns B @range(v1..v2)` postulates if $a : A$ then $`a \in [v_1,v_2]`$ (conditions as before).
* `A value B @values(v1, v2)` postulates if $a : A$ then $`a \in \{v_1, v_2\}`$ , ***requiring*** that: 
  * either $`A : \mathbf{Att}`$, $`A < V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.
  
  **Generalizes** to $n$ values.
* `A value B @regex(v1..v2)` postulates if $a : A$ then $`a`$ conforms with regex `<EXPR>`.
* `A value B @range(v1..v2)` postulates if $a : A$ then $`a \in [v_1,v_2]`$ (conditions as before).

**Case DISTINCT**
* `A owns B[] @distinct` postulates that when $`[b_1, ..., b_n] : [B]`$ then all $`b_i`$ are distinct. 
* `B relates I[] @distinct` postulates that when $`[x_1, ..., x_n] : [I]`$ then all $`x_i`$ are distinct.

### Triggers

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* `(relation) B relates I @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $B$'s cardinality for $I$, triggers deletion of $b$.
  * **defaults** to **TT** error
* `(relation) B @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $B$'s cardinality _for any role_ of $B$, triggers deletion of $b$.
  * **defaults** to **TT** error
* `(attribute) B @independent`. When deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$, update the latter to $`b :_! B`$.
  * **defaults** to: deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$ triggers deletion of $b$.


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
  C2 value V2 (@values(<EXPR>));
```
adds
* _Struct type_ $S : \mathbf{Type}$
* _Struct components_ $`C_1 : \mathbf{Type}`$, $`C_2 : \mathbf{Type}`$, and identify $S = C_1 \times C_2$
* _Component value casting_: $C_1 < V_1$, $C_2 < V_2$
* _Component value terms_: whenever $v : V_i$ then $v : C_i$ if $v$ conforms with `<EXPR>`
  * **defaults** to: whenever $v : V_i$ then $v : C_i$ (no condition)
* **Generalizes** to $n$ components

### Functions defs

**Case STREAM_RET_FUN**
```
fun F (x: T, y: S) -> { A, B }:
  match <PATTERN>
  (<OPERATORS>)
  return { z, w };
```
adds the following to our type system:
* _Function symbol_: $F : \mathbf{Type}(T,S)$.
* _Function type_: when $`x : T`$ and $y: S$ then $F(x:T, y:S) : \mathbf{Type}$
* _Output cast_: $F(x:T, y:S) < A \times B$
* _Function terms_: $`(z,w) : F(x:T, y:S)`$ are discussed in section "Function semantics"
* **Generalizes** to $n$ inputs and $m$ outputs

**Case SINGLE_RET_FUN**
```
fun f (x: T, y: S) -> A, B:
  match <PATTERN>
  (<OPERATORS>)
  return <AGG>, <AGG>;
```
adds the following to our type system:
* _Function symbol_: when $`x : T`$ and $y: S$ then $f(x:T, y:S) : A \times B$
* _Function terms_: $`(z,w) : f(x:T, y:S)`$ are discussed in section "Function semantics"
* **Generalizes** to $n$ inputs and $m$ outputs

_Comment: notice difference in capitalization between the two cases!_

## Undefine semantics

### Types, subtypes, dependencies

**Case ENT**
* `entity A` removes $`A : \mathbf{Ent}`$
* `sub B from (entity) A` removes $`A < B`$

**Case REL**
* `relation A` removes $`A : \mathbf{Rel}`$
* `sub B from (relation) A` removes $`A < B`$
* `relates I from (relation) A` removes $`A : \mathbf{Rel}(I)$
* `as J from (relation) A relates I` removes $`I <_! J`$ 
* `relates I[] from (relation) A` removes $`A : \mathbf{Rel}([I])$
* `as J[] from (relation) A relates I[]` removes $`I <_! J`$

**Case ATT**
* `attribute A` removes $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$
* `value V from (attribute) A value V` removes $`A < V`$
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
* `@values(v1, v2) from A values B` 
* `@range(v1..v2) from A values B`

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
    * $S$ is used in another struct
    * $S$ is used as value type of an attribute

### Functions defs

**Case STREAM_RET_FUN**
* `fun F;`
  removes $F$ and all associated defs.
  * **TT** error if
    * $S$ is used in another function


**Case SINGLE_RET_FUN**
* `fun f;`
  removes $f$ and all associated defs.
  * **TT** error if
    * $S$ is used in another function

_Comment: notice difference in capitalization between the two cases!_

## Redefine semantics

_In each case, redefine acts like an undefine (which cannot be a no-op) and a define of the given axiom_

**Redefine principles**: 

1. Can only redefine one thing at a time
2. We disallow redefining boolean properties:
  * _Example 1_: a type can either exists or not. we cannot "redefine" it's existence, but only define or undefine it.
  * _Example 2_: a type is either abstract or not. we can only define or undefine `@abstract`.

### Types, subtypes, dependencies

**Case ENT**
* cannot redefine `entity A`
* `(entity) A sub B` redefines $`A < B`$

**Case REL**
* cannot redefine `relation A` 
* `(relation) A sub B` redefines $`A < B`$
* cannot redefine `(relation) A relates I`
* `(relation) A relates I as J$` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role
* cannot redefine `(relation) A relates I[]`
* `(relation) A relates I[] as J[]` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role

**Case ATT**
* cannot redefine `attribute A`
* `(attribute) A value V` redefines $`A < V`$
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

cannot redefine structs

### Functions defs

**Case STREAM_RET_FUN**

cannot redefine stream-return functions.

**Case SINGLE_RET_FUN**

cannot redefine single-return functions.

# Data instance languages

This section first describes the satisfication semantics of match queries, obtained by substituting _variables_ in _patterns_ by concepts (_answers_) such that these patterns are _satisfied_. It is then described how instance in ERA types can be declared and further manipulated. Finally, the section describes the semantics of functions (the novelty over match semantics is the ability to declared functions recursively).

## Match semantics

### Basics: Variables and Answers

**Variables**

* _Syntax_: vars start with `$`
* _Usage_: vars appear as part of 
  * statements: indivisible patterns (see Glossary), `;`-separated 
  * patterns: collection of statements, combined with logical connectives:
    * "and" (default), 
    * `or`, 
    * `not`, 
    * `try`
* _Var kinds_: Position in a statement determines wether variables are
  * type variables (**tvar**, uppercase convention in this spec)
  * data instance variables (**evar**, lowercase convention in this spec)

**Answers**

* _Definition_: 

  An *answer* to a pattern means the following
  * _Concept map_: it resolves each variable `$x` to a **concept** `concept($x)` (math. notation: $\gamma(x)$).
  * _Element typing_: For each **evar** `$x` we further record a type `type($x)` (math. notation: $\tau(x)$), such that
    * either $\tau(x)$ is schema type and $\gamma(x) :_! \tau(x)$
    * or $\tau(x)$ is a value type (primitive of structured) and $\gamma(x) :_! \tau(x)$.
  * _Satisfaction_: The concept map and element typing is such that the pattern is **satisfied** (this is defined in the next sections)
* _Key properties_:
  * **tvar**s `$T` always resolve to types
  * **evar**s `$x` always resolve to elements
  * **tvar**s `$T` never resolve to list types
  * **evar**s `$x` may resolve to lists, but this is always indicated in the pattern as we will see.
  * _Theorem_: Based on the rules outlined in the next sections, there is a unique minimal solution for the type assignment $x \mapsto \tau(x)$. Algorithm to compute this:
    1. To each concept assign its direct type if available,
    2. To any attribute instance used in non-comparable, same-valued attribute types otherwise re-assign that value type.

**Optional variables**

_Key principle_:

* If variables are used only in specific positions (called **optional positions**) of patterns, then they are optional variables.
* Only **evar**s can be optional
* A optional variable `$x` is allowed to have the empty concept assigned to it in an answer: $\gamma(x) = \emptyset$.

**Partial answers convention**

In the next section, we describe the answer satisfying statement. By convention (and for brevity), we always consider **fully variablized** statements (e.g. `$x isa $X`, `$X sub $Y`). Indeed, this also determines valid answers to **partially answered** versions of these statements (e.g. `$x isa A`, `$X sub A`, `A sub $Y`) by assigning this partial answers to the variables in the fully variablized statement versions.

**Variable bindings convention**

_Key principle_:

* A pattern will only be accepted by TypeDB if all variables are **bound**. (Otherwise, we may encounter unbounded/literally impossible computations of answers.)
* A variable is bound if it appears in a _binding position_ of at least one statement. 
(Most statements bind their variables: in the next section we highlight non-binding positions)

**Type checking**

_Remark_: _Type Checking_ preceeds answer computation. Type checking failure (**TCF**) will occur when a variables cannot possibly have an assigned type.

### Satisfying type patterns

**Case TYPE_DEF**
* `type $A` (for `type` in `{entity, relation, attribute}`) satisfied if $`\gamma(A) : \mathbf{Type}`$

* `(type) $A sub $B` satisfied if $`\gamma(A) : \mathbf{Type}`$, $`\gamma(B) : \mathbf{Type}`$, $`\gamma(A) \lneq \gamma(B)$
* `(type) $A sub! $B` satisfied if $`\gamma(A) : \mathbf{Type}`$, $`\gamma(B) : \mathbf{Type}`$, $`\gamma(A) <_! \gamma(B)$

_Remark_: `sub!` is convenient, but could actually be expressed with `sub`, `not`, and `is`. Similar remarks apply to **all** other `!`-variations of TypeQL key words below.

**Case REL_PATT**
* `$A relates $I` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma(I))`$

* `$A relates! $I` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma(I))`$ and **not** $`\gamma(A) \lneq \gamma(B) : \mathbf{Rel}(\gamma(I))`$
* `$A relates $I as $J` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma(I))`$, $`B : \mathbf{Rel}(\gamma(J))`$, $`A < B`$, $\gamma(I) < \gamma(J)$.
* `$A relates $I[]` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma([I]))`$
* `$A relates! $I[]` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma([I]))`$ and **not** $`\gamma(A) \lneq \gamma(B) : \mathbf{Rel}(\gamma([I]))`$
* `$A relates $I[] as $J[]` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma([I]))`$, $`B : \mathbf{Rel}(\gamma([J]))`$, $`A < B`$, $\gamma(I) < \gamma(J)$.

**Case PLAY_PATT**
* `$A plays $I` satisfied if $`\gamma(A) < A' <_! \gamma(I)`$ (for $A'$ **not** an interface type)
* `$A plays! $I` satisfied if $`\gamma(A) <_! \gamma(I)`$

**Case OWNS_PATT**
* `$A owns $B` satisfied if $`\gamma(A) < A' <_! \gamma(O_B)`$ (for $A'$ **not** an interface type)
* `$A owns! $B` satisfied if $`\gamma(A) <_! \gamma(O_B)`$ 

_Remark_. In particular, if `A owns B[]` has been declared, then `$X owns B` will match the answer `ans($X) = A`.

### Satisfying constraint patterns

_Remark: the usefulness of constraint patterns seems overall low, could think of a different way to retrieve full schema or at least annotations (this would be more useful than, say,having to find cardinalities by "trialing and erroring" through matching). TODO: discuss!_

**Case CARD_PATT**
* cannot match `@card(n..m)` (TODO: discuss! `@card($n..$m)`??)
<!-- 
* `A relates I @card(n..m)` satisfied if $`\gamma(A) : \mathbf{Rel}(\gamma(I))`$ and schema allows $|a|_I$ to be any number in range `n..m`.
* `A plays B:I @card(n..m)` satisfied if ...
* `A owns B @card(n...m)` satisfied if ...
* `$A relates $I[] @card(n..m)` satisfied if ...
* `$A owns $B[] @card(n...m)` satisfied if ...
-->

**Case PLAYS_AS_PATT**

_Notation: for readability, we simply write $X$ in place of $\gamma(X)$ in this case and the next._

* `$A plays $B:$I as $C:$J` satisfied if $A \leq A' <_! D' \leq D$ for some $D$s, and $I \leq I' <_! J' \leq J$, with $`A^{(')} < {I^{(')}}`$, $`D^{(')} < {J^{(')}}`$, and schema directly contains the constraint `A' plays B':I' as C':J'` for relation types $B \leq B' \leq_! C' \leq C$.

**Case OWNS_AS_PATT**
* `$A owns $B as $C` satisfied if $A \leq A' <_! D' \leq D$ for some $D$s, and $B \leq B' <_! C' \leq C$, with $`A^{(')} < O_{B^{(')}}`$, $`D^{(')} < O_{C^{(')}}`$, and schema directly contains the constraint `A' owns B' as C'`.

_Remark: these two are still not a natural constraint, as foreshadowed by a previous remark!_

**Case UNIQUE_PATT**
* `$A owns $B @unique` satisfied if $`\gamma(A) < A' <_! \gamma(O_B)`$ (for $A'$ **not** an interface type), and schema directly contains constraint `A' owns ans($B) @key`.

* `$A owns! $B @unique` satisfied if $`\gamma(A) <_! \gamma(O_B)`$, and schema directly contains constraint `ans($A) owns ans($B) @unique`.

**Case KEY_PATT**
* `$A owns $B @key` satisfied if $`\gamma(A) < A' <_! \gamma(O_B)`$ (for $A'$ **not** an interface type), and schema directly contains constraint `A' owns ans($B) @key`.

* `$A owns! $B @key` satisfied if $`\gamma(A) <_! \gamma(O_B)`$, and schema directly contains constraint `ans($A) owns ans($B) @key`.

**Case SUBKEY_PATT**
* `$A owns $B @subkey(<LABEL>)` satisfied if $`\gamma(A) < A' <_! \gamma(O_B)`$ (for $A'$ **not** an interface type), and schema directly contains constraint `A' owns ans($B) @subkey(<LABEL>)`.

**Case ABSTRACT_PATT**
* `(type) $B @abstract` satisfied if schema directly contains `(type) ans($B) @abstract`.
* `$A plays $B:$I @abstract` satisfied if $`\gamma(A) < A'`$, $`\gamma(B) : \mathbf{Rel}(\gamma(I))`$, $`\gamma(B) < B' : \mathbf{Rel}(\gamma(I))`$ and schema directly contains constraint `A' plays B':ans($I) @abstract`.
* `$A owns $B @abstract` satisfied if $`\gamma(A) < A'`$ and schema directly contains one of the constraints
  * `A' owns ans($B) @abstract`
  * `A' owns ans($B)[]`

* `$A owns $B[] @abstract` satisfied if $`\gamma(A) < A'`$ and schema directly contains constraint `A' owns ans($B)[] @abstract`.
* `$B relates $I @abstract` satisfied if $`B : \mathbf{Rel}(I)`$, $`\gamma(B) < B'`$, and schema directly contains constraint `B' relates ans($I) @abstract`.
* `$B relates $I[] @abstract` satisfied if $`B : \mathbf{Rel}([I])`$, $`\gamma(B) < B'`$, and schema directly contains constraint `B' relates ans($I)[] @abstract`.

**Case VALUES_PATT**
* cannot match `@values/@regex/@range` (TODO: discuss!)
<!--
* `A owns B @values(v1, v2)` satisfied if 
* `A owns B @regex(<EXPR>)` satisfied if 
* `A owns B @range(v1..v2)` satisfied if 
* `A value B @values(v1, v2)` satisfied if 
* `A value B @regex(<EXPR>)` satisfied if 
* `A value B @range(v1..v2)` satisfied if 
-->

**Case DISTINCT_PATT**
* `A owns B[] @distinct` satisfied if $`\gamma(A) < A' <_! \gamma(O_B)`$ (for $A'$ **not** an interface type), and schema directly contains constraint `A' owns ans($B)[] @distinct`.
* `B relates I[] @distinct` satisfied if $`\gamma(B) : \mathbf{Rel}(\gamma([I]))`$, $`B < B'`$ and schema directly contains `B' relates I[] @distinct`.

### Satisfying data patterns

**Case ISA_PATT**
* `$x isa $T` satisfied if $`\gamma(x) : \gamma(T)`$ for $`\gamma(T) : \mathbf{ERA}`$
* `$x isa! $T` satisfied if $`\gamma(x) :_! \gamma(T)`$ for $`\gamma(T) : \mathbf{ERA}`$

**Case LINKS_PATT**
* `$x links ($I: $y)` satisfied if $`\gamma(x) : A(\gamma(y):\gamma(I))`$ for some $`A : \mathbf{Rel}(\gamma(I))`$.

**Case HAS_PATT**
* `$x has $B $y` satisfied if $`\gamma(y) : \gamma(B)(\gamma(x):O_{\gamma(B)})`$ for some $`B : \mathbf{Att}`$.

**Case IS_PATT**
* `$x is $y` satisfied if $`\gamma(x) :_! A`$, $`\gamma(y) :_! A`$, $`\gamma(x) = \gamma(y)`$, for $`A : \mathbf{ERA}`$
* `$A is $B` satisfied if $`A = B`$ for $`A : \mathbf{ERA}`$, $`B : \mathbf{ERA}`$

***System property***

1. In the `is` pattern, left or right variable are **not bound**.

_Remark_: In the `is` pattern we cannot syntactically distinguish whether we are in the "type" or "element" case, but this is alleviated by the pattern being non-binding: statements which bind these variables determine which case we are in.

### Satisfying value expression patterns

_Expression grammar_

```
<BOOL>      ::= VAR | true | false | BOOL_LIST[INT]
<INT>       ::= VAR | int | ( INT ) | INT + INT | INT - INT | INT_LIST[INT]
<STRING>    ::= VAR | string | string + string | STRING_LIST[INT]
<T_LIST>    ::= VAR | [T,...,T] | T_LIST + T_LIST    // note: includes empty list []
<INT_LIST>  ::= VAR | INT_LIST | [INT..INT]
<DATETIME>  ::= ...
<TIME>      ::= ...
<LIST_EXPR> ::= T_LIST     // for any T
<EXPR>      ::= BOOL | INT | STRING | T_LIST | ...
```

**Case ASS_PATT**
* `$x = <EXPR>` is satisfied if $`\gamma(x)`$ equals the expression on the right-hand side, evaluated after substituting answer for all its variables.

***System property***

1. Any variable appearing `<EXPR>` is **not bound** by the pattern (and must be bound elsewhere). The left-hand variable is bound by the pattern.
2. It must be possibly to determine answers of all variables in `<EXPR>` before answering `$x` — this avoids cyclic assignments (like `$x = $x + $y; $y = $y - $x;`)

**Case IN_LIST_PATT**
* `$x in $l` is satisfied if $`\gamma(l) : [A]`$ for $`A : \mathbf{Type}`$ and $`\gamma(x) \in \gamma(l)`$
* `$x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` 

***System property***

1. The right-hand side variable(s) of the pattern are **not bound**. (The left-hand side variable is bound.)

**Case EQ_PATT**
* `$x == $y` is satisfied if $`\gamma(x) : V`$, $`\gamma(y) : V`$ for a value type $`V`$ (either primitive or struct), and $`\gamma(x) = \gamma(y)`$
* `$x != $y` is equivalent to `not { $x == $y }` (see "Satisfying composite patterns")

***System property***

1. In the `==` pattern left or right variable are **not bound**.

**Case COMP_PATT**

The following are all kind of obvious.

* `<INT> <COMP> <INT>` 
* `<BOOl> <COMP> <BOOL>` (`false` < `true`)
* `<STRING> <COMP> <STRING>` (lexicographic comparison)
* `<DATETIME> <COMP> <DATETIME>` (usual datetime order)
* `<TIME> <COMP> <TIME>` (usual time order)
* `<STRING> contains <STRING>` 
* `<STRING> like <REGEX>` (where `<REGEX>` is a regex string without variables)

***System property***

1. In all the above patterns all their variables are **not bound**.

### Satisfying function patterns

**Case IN_FUN_PATT**
* `$x, $y?, ... = <FUN_CALL>` is satisfied if substituting answers in `<FUN_CALL>` yields a **function answer set** $F$ (see "Function semantics") of tuples $t$, and for some tuple $t \in F$ we have:
  * for the $i$th variable `$z`, which is non-optional, we have $`\gamma(z) = t_i`$
  * for the $i$th variable `$z`, which is marked as optional using `?`, we have either
    * $\gamma(z) = t_i$ and $t_i \neq \emptyset$
    * $\gamma(z) = t_i$ and $t_i = \emptyset$

**Case ASS_FUN_PATT**
* `$x, $y?, ... = <FUN_CALL>` is satisfied if substituting answers in `<FUN_CALL>` yields a **function answer tuple** $f$ (see "Function semantics") and we have:
  * for the $i$th variable `$z`, which is non-optional, we have $`\gamma(z) = t_i`$
  * for the $i$th variable `$z`, which is marked as optional using `?`, we have either
    * $\gamma(z) = t_i$ and $t_i \neq \emptyset$
    * $\gamma(z) = t_i$ and $t_i = \emptyset$

_Remark_: variables marked with `?` in function assignments are the first example of **optional variables**. We will meet other pattern yielding optional variables in the following section.


### Satisfying composite patterns

Now that we have seen how to determine when answers satisfy individual statements, we can extend our discussion of match semantics to full pattern.

**Case AND_PATT**
* An answer satisfies the pattern `<PATT1>; <PATT2>;` that simultaneously satisfies both `<PATT1>` and `<PATT2>`.


**Case OR_PATT**
* An answer for the pattern `{ <PATT1> } or { <PATT2> };` is an answer that satisfies either `<PATT1>` or `<PATT2>`.

_Remark_: this generalize to a chain of $k$ `or` clauses.

**Case NOT_PATT**
* An answer satisfying the pattern `not { <PATT> };` is any answer which _cannot_ be completed to a answer satisfying `<PATT>`.

**Case TRY_PATT**
* The pattern `try { <PATT> };` is equivalent to the pattern `{ <PATT> } or { not { <PATT>}; };`.

### Answer sets

Given a database (data model + data), define the _answer set_ `ans(<PATT>)` to be the set of **proper minimal** answers that satisfy the pattern `<PATT>`.

_Definition_. Here, "proper minimal" means 
* we cannot remove variables from the answer concept map without dissatisfying `<PATT>`
* all variables in the answer concept map appear outside of a `not` block at least once. 

_Example_: Consider the pattern `$x isa Person;` (this pattern comprises a single statement). Than `($x -> p)` satisfies the pattern if `p` is an element of the type `Person` (i.e. $p : \mathsf{Person}$). The answer `($x -> p, $y -> p)` also satisfies the pattern, but it is not proper minimal.

_Note: we will discuss the insertion of data and relevant system properties in "Insert semantics"._


## Function semantics

### Function signature, body, operators

**case FUN_SIGN_STREAM**

_Syntax_:
```
fun F ($x: A, $y: B[]) -> { C, D[], E? } :
```
where
* types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_TODO: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

**case FUN_SIGN_SINGLE**

_Syntax_:
```
fun F ($x: A, $y: B[]) -> C, D[], E? :
```
where
* types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_TODO: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

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
  * performs a `select` of the listed variables
  * return resulting concept map set

### Single-return

* `return <AGG>, <AGG>, ...;` where `<AGG>` is one of the following **aggregate functions**:
  * `check`:
    * output type `bool`
    * returns `true` if concept map set non-empty
  * `sum($x)`:
    * output type `float` or `int`
    * return sum of all `ans($x)` in answer set
    * `$x` can be optional
    * empty sums yield `0.0f` or `0`
  * `mean($x)`:
    * output type `float?`
    * return mean of all `ans($x)` in answer set
    * `$x` can be optional
    * empty mean return $\emptyset$
  * `median($x)`, 
    * output type `float?` or `int?` (depending on type of `$x`)
    * return median of all `ans($x)` in answer set
    * `$x` can be optional
    * empty medians return $\emptyset$
  * `first($x)`
    * `A?` for any `A`
    * return sum of all `ans($x)` in answer set
    * `$x` can be optional
    * if no `ans($x)`is set, return $\emptyset$
  * `count`
    * output type `long`
    * return count of all answers
  * `count($x)`
    * output type `long`
    * return count of all `ans($x)` in answer set
    * `$x` can be optional
* Each `<AGG>` reduces the final concept map set to a single value
  * These reduction operations are described in "Reduce"

### Recursion

Functions can be called recursively, as long as negation can be stratified. The semantics in this case is computed "stratum by stratum". 

## Insert semantics

### 

### Leaf attribute system constraint

TODO: Remove this constraint!

## Delete semantics

## Update semantics

## Put semantics


# Pipelines

## Basics of streams

Stream are ordered concept map sets
Eager evaluation

## Clauses

### Match

### Insert

### Delete

### Update

### Fetch

## Operators

### Select 

### Sort

### Limit

### Offset

_Remark_: Offset is only useful when streams (and the order of answers) are fully deterministic.

### Reduce

## Execution

Execution order of pipelines

# Transactionality and Concurrency

## Basics

## Snapshots

## Isolation

# Sharding

(TBD)

Optional fields in structs

sticky: behaviour of `abstract`

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

The set of concept maps that satisfy a pattern.

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

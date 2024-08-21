# TypeDB - Behaviour Specification

**Table of contents**

- [Terminology and notation](#terminology-and-notation)
  - [Terminology](#terminology)
  - [Type basics and notation](#type-basics-and-notation)
- [Schema](#schema)
  - [Basics](#basics)
  - [Define semantics](#define-semantics)
    - [Type defs](#type-defs)
    - [Constraints](#constraints)
    - [Triggers](#triggers)
    - [Value type defs](#value-type-defs)
    - [Functions defs](#functions-defs)
  - [Undefine semantics](#undefine-semantics)
    - [Type defs](#type-defs-1)
    - [Constraints](#constraints-1)
    - [Triggers](#triggers-1)
    - [Value type defs](#value-type-defs-1)
    - [Functions defs](#functions-defs-1)
  - [Redefine semantics](#redefine-semantics)
    - [Type defs](#type-defs-2)
    - [Constraints](#constraints-2)
    - [Triggers](#triggers-2)
    - [Value type defs](#value-type-defs-2)
    - [Functions defs](#functions-defs-2)
- [Data languages](#data-languages)
  - [Match semantics](#match-semantics)
    - [Optionality](#optionality)
    - [Function semantics](#function-semantics)
  - [Insert semantics](#insert-semantics)
  - [Delete semantics](#delete-semantics)
  - [Update semantics](#update-semantics)
  - [Put semantics](#put-semantics)
  - [Function semantics](#function-semantics-1)
- [Pipelines](#pipelines)
  - [Basics of streams](#basics-of-streams)
  - [Clauses](#clauses)
  - [Operators](#operators)
  - [Execution](#execution)
- [Transactionality and Concurrency](#transactionality-and-concurrency)
  - [Basics](#basics-1)
  - [Snapshots](#snapshots)
  - [Isolation](#isolation)
- [Sharding](#sharding)


# Terminology and notation

_(For reference only)_

## Terminology

* **TT** — transaction time
  * _Interpretation_: any time within transaction
* **CT** — commit time
  * _Interpretation_: time of committing a transaction to DB
* **tvar** — type variable
* **dvar** - data variable

_Note_: **tvar**s and **dvar**s are uniquely distinguish everywhere in TypeQL

## Type system basics and notation

* **Types and Typing**. $`A : \mathbf{Type}`$ means
  > $A$ is a type. 
  
  If $A$ is a type, then we may write $`a : A`$ to mean
  > $a$ is an element in type $A$.

  * _Direct typing_: $a :! A$ means:
    > $a$ was declared as an element of $A$ by the user (a.k.a. a _direct_ typing).

  * _Example_: $p$ is of type $\mathsf{Person}$
* **Dependent types**. Write $`A : \mathbf{Type}(I,J,...)`$ to mean
  > $A$ is a type with interface types $`I, J, ...`$.

  * _Variations_: may replace $\textbf{Type}$ by $\mathbf{Ent}$, $\mathbf{Rel}$, $\mathbf{Obj} = \mathbf{Ent} + \mathbf{Rel}$, or $\mathbf{Att}$
  * _Example_: $`\mathsf{Person} : \mathbf{Ent}`$ is an entity type.
  * _Example_: $`\mathsf{Marriage : \mathbf{Rel}(Spouse)}`$ is a relation type with interface type $`\mathsf{Spouse}`$.
* **Dependent typing**. $A : \mathbf{Type}(I,J,...)$ implies $`A(x:I, y:J) : \mathbf{Type}`$ for any $x: I, y: J$. Writing $`a : A(x : I, y : J,...)`$ means:
  > $a$ is an element in type "$`A`$ of $`x`$ (as $`I`$), and $`y`$ (as $`J`$), and ...".

  * _Grouping duplicates_: $`a : A(x : I, y : I)`$ write $`A : A(\{x,y\}:I^2)`$
  * _Role cardinality_: $|a|_I$ counts elements in $\{x_1,...,x_k\} :I^k$
  * **Example**: $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$. Then $|m|_{\mathsf{Spouse}} = 2$.
* **Key properties of dependencies**
  * _Combining dependencies_: if $A : \mathbf{Type}(I)$ and $A : \mathbf{Type}(J)$ then $A : \mathbf{Type}(I,J)$
    * _Remark_: This applies recursively.
    * _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$
  * _Weakening dependencies_: If $A : \mathbf{Type}(I,J)$ then $A : \mathbf{Type}(I)$
    * _Remark_: This applies recursively.
    * _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse^2})`$ then $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and $`\mathsf{Marriage} : \mathbf{Rel}`$ (omit "$`()`$")
* **Casting**. Write $`A < B`$ to indicating type casts from $A$ to $B$, i.e. if $a : A$ then $a : B$. In other words:
  > A casts into B

  * _Direct castings_: Write $`A <_! B`$ mean:
    > A cast from A to B was declared by user (a.k.a. _direct_ cast) from A to B.

    * _Transitive closure_: $`A <_! B`$ implies $`A < B`$, the latter is transitive
    * _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$
    * _Example_: $`\mathsf{Child} <_! \mathsf{Nameowner}`$
    * _Example_: $`\mathsf{Person} <_! \mathsf{Spouse}`$
  * _Weakening dependencies_: If $`a : A(x:I, y:J)`$ then $`a : A(x:I)`$. In other words:
    > Elements in $`A(I,J)`$ casts into elements of $`A(I)`$.

    * _Remark_: This applies recursively.
    * _Remark 2_: This casting preserves direct typings! I.e. when $`a :! A(x:I, y:J)`$ then $`a :! A(x:I)`$
    * _Example_: If $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$ then both $m : \mathsf{Marriage}(x:\mathsf{Spouse})$ and $m : \mathsf{Marriage}(y:\mathsf{Spouse})$
  * _Covariance of dependencies_: Given $`A < B`$, $`I < J`$ such that $`A : \mathbf{Type}(I)`$ $`B : \mathbf{Type}(J)`$, then $`a : A(x:I)`$ implies $`a : B(x:J)`$; in other words:
    > Elements in $`A(I)`$ cast into elements of $`B(J)`$.

    * _Remark_: This applies recursively.
    * _Example_: If $m : \mathsf{HeteroMarriage}(x:\mathsf{Husband}, y:\mathsf{Wife})$ then $m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)$
* **List types**. $`[A] : \mathbf{Type}`$ — List type of $A$ (contains lists $`[a_0, a_1, ...]`$ of $`a_i : A`$)
  * _Dependency on list types_: We allow $`A : \mathbf{Type}([I])`$.
    * _Example_: $`\mathsf{FlightPath} : \mathbf{Rel}([\mathsf{Flight}])`$
  * _Dependent list types_: We allow $`[A] : \mathbf{Type}(I)`$, i.e. $`[A](x:I) : \mathbf{Type}`$.
    * _List terms_: Enforce $`[A](x:I) < [A]`$, in other words:
    > Every element $l$ of $`[A](x:I)`$ is actually an $A$-list $`l : [A]`$.

    * _Example_: $`[a,b,c] : [\mathsf{MiddleName}](x : \mathsf{MiddleNameListOwner})`$
  * _List length_: for list $l : [A]$ the term $\mathrm{len}(l) : \mathbb{N}$ represents $l$'s length
  * _Abstractness_: all list types are abstract by default, i.e. their terms cannot be explicitly declared (a la $`l :! [A](x:I)`$)
* **Sum types**. $`A + B`$ — Sum type
* **Product types**. $`A \times B`$ — Product type
* **Type cardinality**.$`|A| : \mathbb{N}`$ — Cardinality of $A$

_Remark for nerds: list types are neither sums, nor products, nor polynomials ... they are so-called _inductive_ types!_


# Schema

***Pertaining to defining and manipulating TypeDB schemas***

## Basics

* Categories of definition clauses:
  * `define`: always adds type axioms or conditions
  * `undefine`: removes type axioms or conditions
  * `redefine`: always both removes and (re-)adds
* Schema components:
  * **Schema type defs**: data-capturing types and type dependencies.
  * **Constraints**: postulate conditions that the system satisfies.
  * **Triggers**: actions to be executed based on certain conditions.
  * **Value type defs**: compositions of primitive value types.
  * **Function defs**: composite data-capturing types
* For execution and validation of definitions see "Transactionality" section
* Definition clauses can be chained:
  * _Example_ 
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

### Type defs

**Case ENT**
* `entity A` adds $`A : \mathbf{Ent}`$
* `(entity) A sub B` adds $`A : \mathbf{Ent}, A <_! B`$

**Case REL**
* `relation A` adds $`A : \mathbf{Rel}`$
* `(relation) A sub B` adds $`A : \mathbf{Rel}, A <_! B`$ (**require**: $`B : \mathbf{Rel}`$ and not $A <_! C \neq B$)
* `(relation) A relates I` adds $`A : \mathbf{Rel}(I)$
* `(relation) A relates I as J` adds $`A : \mathbf{Rel}(I)`$, $`I <_! J`$ (**require**: $`B : \mathbf{Rel}(J)`$ and $A <_! B$)
* `(relation) A relates I[]` adds $`A : \mathbf{Rel}([I])$
* `(relation) A relates I[] as J[]` adds $`A : \mathbf{Rel}([I])`$, $`I <_! J`$ (**require**: $`B : \mathbf{Rel}([J])`$ and $A <_! B$)

**Case ATT**
* `attribute A` adds $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$ ($O_A$ being automatically generated ownership interface)
* `(attribute) A value V` adds $`A < V`$ (**require**: $V$ a primitive or struct value type)
* `(attribute) A sub B` adds $`A : \mathbf{A}`$, $`A : \mathbf{Att}(O_A)`$, $`A <_! B`$ and $`O_A <_! O_B`$ (**require**: $`B : \mathbf{Att}(O_A)`$ and not $A <_! C \neq B$)

**Case PLAYS**
* `A plays B:I` adds $`A <_! I`$ (**require**: $B: \mathbf{Rel}(I)$, $`A :\mathbf{Obj}`$)

**Case OWNS**
* `A owns B` adds $`A <_! O_B`$ (**require**: $B: \mathbf{Att}(O_B)$, $`A :\mathbf{Obj}`$)
* `A owns B[]` adds $`A <_! O_B`$ (**require**: $B: \mathbf{Att}(O_B)$, **puts B[] to be non-abstract**: i.e. allows declaring terms $`l :! [B](x:O_B)`$, see earlier discussion of list types)

_Note: based on recent discussion, `A owns B[]` _implies_ `A owns B @abstract` (abstractness is crucial here, see `abstract` constraint below). See **match semantics**._

### Constraints

**Case CARD**
* `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $a : A(\{...\} : I^k)$ (for maximal $k$, fixed $a : A$)
  * **defaults** to `@card(1..1)` if omitted ("one")
* `A plays B:I @card(n..m)` postulates $n \leq |B(a:I)| \leq m$ for all $a : A$
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B @card(n...m)` postulates $n \leq |B(a:I)| \leq m$ for all $a : A$
  * **defaults** to `@card(0..1)` if omitted ("one or null")
  
_Note 1: Upper bounds can be omitted, writing `@card(2..)`, to allow for arbitrary large cardinalities_

_Note 2: For cardinality, and for most other constraints, we should reject redundant conditions, such as `A owns B card(0..3);` when `A sub A'` and `A' owns B card(1..2);`_

**Case CARD_LIST**
* `A relates I[] @card(n..m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $a : A(l : [I])$
  * **defaults** to `@card(0..)` if omitted ("many")
* `A owns B[] @card(n...m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`l : [B](a:O_B)`$ for $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")

**Case PLAYS_AS**
* `A plays B as C` postulates $`|C(x:O_C)| - |B(x:O_B)| = 0`$ for all $`x:A`$ (**require**: $B \lneq C$, $A < D$, $`D <_! C`$). **Invalidated** when $A <_! C'$ for $B \lneq C' \leq C$.

**Case OWNS_AS**
* `A owns B as C` postulates $`|C(x:O_C)| - |B(x:O_B)| = 0`$ for all $`x:A`$ (**require**: $B \lneq C$, $A < D$, $`D <_! O_C`$). **Invalidated** when $A <_! O_{C'}$ for $B \lneq C' \leq C$.

_Comment: both preceding cases are kinda complicated/unnatural ... as reflected by the math._

**Case UNIQUE**
* `A owns B @unique` postulates that if $`b : B(a:O_B)`$ for some $a : A$ then this $a$ is unique (for fixed $b$).

**Case KEY**
* `A owns B @key` postulates that if $`b : B(a:O_B)`$ for some $a : A$ then this $a$ is unique, and also $|B(a:O_B) = 1$.

**Case SUBKEY**
* `A owns B1 @subkey(<LABEL>); A owns B2 @subkey(<LABEL>)` postulates that if $b : B_1(a:O_{B_1}) \times B_2(a:O_{B_2})`$ for some $a : A$ then this $a$ is unique, and also $|B_1(a:O_{B_1}) \times B_2(a:O_{B_2})| = 1$. **Generalizes** to $n$ subkeys.

**Case ABSTRACT**
* `(type) B @abstract` postulates $`b :! B(...)`$ to be impossible
* `A plays B:I @abstract` postulates $`b :! B(a:I)`$ to be impossible for $a : A$
* `A owns B @abstract` postulates $`b :! B(a:I)`$ to be impossible for $a : A$ 
* `B relates I @abstract` postulates $`A <_! I`$ to be impossible for $A : \mathbf{Obj}$

_Comment: The last case is the ugly duckling. Revisit?_

**Case VALUES**
* `A owns B @values(v1, v2)` postulates if $a : A$ then $`a \in \{v_1, v_2\}`$  (**require**: 
  * either $`A : \mathbf{Att}`$, $`A < V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.) 
  
  **Generalizes** to $n$ values.
* `A owns B @values(v1..v2)` postulates if $a : A$ then $`a \in [v_1,v_2]`$ (conditions as before).
* `A value B @values(v1, v2)` postulates if $a : A$ then $`a \in \{v_1, v_2\}`$  (**require**: 
  * either $`A : \mathbf{Att}`$, $`A < V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.)
  
  **Generalizes** to $n$ values.
* `A value B @values(v1..v2)` postulates if $a : A$ then $`a \in [v_1,v_2]`$ (conditions as before).

**Case DISTINCT**
* `A owns B[] @distinct` postulates that when $`[b_1, ..., b_n] : [B]`$ then all $`b_i`$ are distinct. 
* `B relates I[] @distinct` postulates that when $`[x_1, ..., x_n] : [I]`$ then all $`x_i`$ are distinct.

### Triggers

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* `(relation) B relates I @cascade`: deleting $`a : A`$ with existing $`b :! B(a:I,\Gamma)`$, such that $`b :! B(\Gamma)`$ violates $B$'s cardinality for $I$, triggers deletion of $b$.
  * **defaults** to **TT** error
* `(relation) B @cascade`: deleting $`a : A`$ with existing $`b :! B(a:I,\Gamma)`$, such that $`b :! B(\Gamma)`$ violates $B$'s cardinality _for any role_ of $B$, triggers deletion of $b$.
  * **defaults** to **TT** error
* `(attribute) B @independent`. When deleting $`a : A`$ with existing $`b :! B(a:O_B)`$, update the latter to $`b :! B`$.
  * **defaults** to: deleting $`a : A`$ with existing $`b :! B(a:O_B)`$ triggers deletion of $b$.


### Value type defs

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

### Type defs

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
* `@abstract from B relates I`

**Case VALUES**
* `@values(v1, v2) from A owns B` 
* `@values(v1..v2) from A owns B`
* `@values(v1, v2) from A values B` 
* `@values(v1..v2) from A values B`

**Case DISTINCT**
* `@distinct from A owns B[]`
* `@distinct from B relates I[]`

### Triggers

_In each case, `undefine` removes the triggered action._

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* `@cascade from (relation) B relates I`
* `@cascade from (relation) B`
* `@independent from (attribute) B`

### Value type defs

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

### Type defs

**Case ENT**
* cannot redefine `entity A`
* `(entity) A sub B` redefines $`A < B`$

**Case REL**
* cannot redefine `relation A` 
* `(relation) A sub B` redefines $`A < B`$
* cannot redefine `(relation) A relates I$`
* `(relation) A relates I as J$` redefines $`I <_! J`$ 
* cannot redefine `(relation) A relates I[]`
* `(relation) A relates I[] as J[]` redefines $`I <_! J`$

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
* cannot redefine `B relates I @abstract`

**Case VALUES**
* `A owns B @values(v1, v2)` 
* `A owns B @values(v1..v2)`
* `A values B @values(v1, v2)` 
* `A values B @values(v1..v2)`

**Case DISTINCT**
* cannot redefine `A owns B[] @distinct`
* cannot redefine `B relates I[] @distinct`

### Triggers

_In each case, `redefine` redefines the triggered action._

**Case DEP_DEL (CASCADE/INDEPEDENT)**
* cannot redefine `(relation) B relates I @cascade`
* cannot redefine `(relation) B @cascade`
* cannot redefine `(attribute) B @independent`

### Value type defs

**Case PRIMITIVES**
cannot redefine primitives

**Case STRUCT**
cannot redefine structs

### Functions defs

**Case STREAM_RET_FUN**
cannot redefine stream-return functions.

**Case SINGLE_RET_FUN**
cannot redefine single-return functions.

# Data languages

_Pertaining to reading and writing data_

## Match patterns and semantics

Topics: match semantics, optionality (in structs and function outputs)

### Optionality

### Function semantics

## Insert semantics

## Delete semantics

## Update semantics

## Put semantics

## Function semantics

# Pipelines

## Basics of streams

## Clauses

Fetch, 

## Operators

Select, Reduce, 

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

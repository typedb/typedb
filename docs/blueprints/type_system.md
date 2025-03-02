# The type system

> **Types** are organizational tool of formal languages, and the analogs of **sets** in classical mathematics. 

## Overview

### Purpose

The issue with TypeQL is that meaning is sometimes overloaded/ambiguous. Therefore we need to a more **precise language**: this realized by the type system in this document. Here's a rough dictionary from TypeQL / natural language to symbols in the type system language. ***All symbols will be explained below in detail***.

### Construction dictionary
| TypeQL / NL                             | Type System / Symbols |
|-----------------------------------------|-----------------------|
| `all ent. types`                        | `ENT`                 |
| `all rel. types`                        | `REL`                 |
| `all att. types`                        | `ATT`                 |
| `all trait types`                       | `TRAIT`               |
| `primitive value types`                 | `PRIM`                |
| `all value types` (incl. structs)       | `VAL`                 |
| `all list types`                        | `LIST`                |
| **kind** of type `A`                    | `_kind(T)`            |
| placeholder for **any type "kind"**     | `KIND`                |
| `T = A or B` (sum type)                 | `T = A + B`           |
| `T = (A, B)` (product type)             | `T = (A, B)`          |
| `Option of A`                           | `A?`                  |
| `List of A`                             | `A[]`                 |
| **length** of list `l : A[]`            | `_len(l)`             |
| **membership** of `a` in list `l : A[]` | `a _in l`             |
| **size** of type `A : KIND`             | `_size(A)`            |
| **value** of attribute `a : A`          | `_val(a)`             |

### Keyword dictionary
| TypeQL / NL                            | Type System / Symbols                     |
|----------------------------------------|-------------------------------------------|
| `isa`                                  | `:`                                       |
| `isa!`                                 | `:!`                                      |
| `sub && !=`                            | `<`                                       |
| `sub && ==`                            | `<=`                                      |
| `sub!`                                 | `<!`                                      |
| `kind A`                               | `A : KIND`                                |
| `kind A @abstract`                     | `#(A : KIND)`                             |
| "`A` references `I` when instantiated" | `A : KIND(I)`                             |
| `A relates I`                          | `A : REL(I)`                              |
| `A relates! I`                         | `B : REL(I)` implies `B <= A`             |
| `A relates I @abstract`                | `#(B : REL(I))`                           |
| `A relates I[]`                        | `A : REL(I[])`                            |
| `A relates! I[]`                       | `B : REL(I[])` implies `B <= A`           |
| `A relates I[] @abstract`              | `#(B : REL(I[]))`                         |
| `A owned_by A.O`                       | `A : ATT(A.O)`                            |
| `A[] owned_by A[].O`                   | `A[] : LIST(A[].O)`                       |
| `A value V`                            | `_val : B -> V` for any `A <= B`          |
| `A plays I`                            | `A <= B <! I` for `_kind(A) = _kind(B)`   |
| `A plays! I`                           | `A <! I`                                  |
| `A plays C @abstract`                  | `#(A <! I)`                               |
| `A owns C`                             | `A <= B <! C.O` for `_kind(A) = _kind(B)` |
| `A owns! C`                            | `A <! C.O`                                |
| `A owns C @abstract`                   | `#(A <! C.O)`                             |
| `x links (T: y)`                       | `x : T(y : I)`                            |
| `x links! (T: y)`                      | `x :! T(y : I)`                           |
| `x links (T[]: y)`                     | `x : T(y : I[])`                          |
| `x links! (T[]: y)`                    | `x :! T(y : I[])`                         |
| `x has T y`                            | `y : T(x : T.O)`                          |
| `x has! T y`                           | `y :! T(x : T.O)`                         |
| `x has T[] y`                          | `y : T[](x : T[].O)`                      |
| `x has! T[] y`                         | `y :! T[](x : T[].O)`                     |

### Type system architecture

A type system is a set of rules for a formal language whose statements describe:

* What types (e.g. `integer`) the system contains 
* What terms (e.g.: `0 : integer`) these types contain
* Which relations (e.g. `<`) and functions (e.g. `_val : A -> integer`) between types the system supports

This document introduces TypeDB's type system in two stages (which is common)

* We first introduce and explain the **syntax** of statements in the system.
* We then discuss the **rule system** for inferring which statements are _true_.

TypeDB's type is **open**, in that we allow dynamically adding new true statements ("**axioms**") it. Namely, this happens via definitions and data writes. The rules above can be used to derive statements that are consequences of these statements.

---

## THE SYNTAX

This section explains the main syntax elements of the type system (we **omit** some common syntax, like equality (`a = b`) and **function syntax** (`f(a)`), all **expression syntax** (`a + b`, `a - b` ...), **list syntax** (`a _in l`, `l + h`, `[a, b, ...]`), etc.).

### Types

* **Type symbols and kinds**. We write
  `A : KIND` to mean the statement:
  > `A` is a type of kind `KIND`.

  The type system allows the following cases of **type kinds**, in which we can insert "type symbols" (see small difference with "user-labelled" types below: these exclude `OWN`)
  * `ENT` (collection of **entity types**)
  * `REL` (collection of **relation types**)
  * `ATT` (collection of **attribute types**)
  * `ROL` (collection of **role trait types**)
  * `OWN` (collection of **ownership trait types**)
  * `PRIM` (collection of **primitive value types**)
  
  _**Note**_: all other types (**structs**, **lists**, ...) are constructed by type operations (sums, products, list operator, ...) from these type symbols.

  _**Notation**_: given a type symbol `A` in our type, write `_kind(A)` to extract its type kind from the list above.

  _**Example**_: `Person : ENT` means `Person` an entity type. So `_kind(Peron) = ENT`.

* **Combined kinds notation**. The following are useful abbreviations:
  * `OBJ = ENT + REL` (collection of **object types**)
  * `OBJ} = REL + ATT` (collection of **dependent types**)
  * `ERA = ENT + REL + ATT` (collection of **ERA types**)
  * `VAL = PRIM + STRUCT` (collection of all **value types**),
    * _Note_: a `STRUCT` type is obtained using **product** and **option** type operations on `PRIM`s and other `STRUCT`s
  * `LABEL = ERA + ROL + VAL` (collection of all **user-labeled types**)
  * `TRAIT = ROL + OWN` (collection of all **trait types**)
  * `ALG = OP*(LABEL)` (collection of all **algebraic types**,
    * _Note_: a `ALG`ebraic type is obtained by recursively constructing types under operators: **sum**, **product**, **option**... see "Type operators" below.
    * _Why is `ALG` useful?_
      * main use case of sums: type checking
      * main use case of option types: option types
      * main use case of products: structs (exte)
  
  * `TYPE = ALG + LIST` (collection of all **types**)

* **Dependent type kinds**. We write `A : KIND(I,J,...)` to mean:
  > `A` is a type of kind `KIND` with **trait types** `I, J, ...`.

  The type system allows two cases of **dependent type kinds**:
  * `ATT` (collection of **attribute types**)
    * Attribute traits are called **ownership types**
  * `REL` (collection of **relation types**)
    * Relation traits are called **role types**

  _Example_: `Marriage : REL(Spouse)}` is a relation type with trait type `Spouse : TRAIT`.


### Types and subtypes

We discuss the syntax for statements relating to types, and explain them in natural language statements.

* **Typing**: If `A` is a type, then we may write `a : A` to mean the statement:
  > `a` is an element in type `A`.

  _Example_: `p : Person` means `p` is of type `Person`

* **Direct typing**: We write `a :! A` to mean:
  > `a` was declared directly as an element of `A` by the user (we speak of a ***direct typing***).

  _Remark_. The notion of direct typing might be confusing at first. Mathematically, it is merely an additional statement in our type system. Intuively, you can think of it as a way of keeping track of the _user-provided_ information. A similar remark applies to direct subtyping (`<!`) below.

* **Dependent typing**:  We write `a : A(x : I, y : J,...)` to mean:
  > `a` lives in the type "`A` of `x` (cast as `I`), and `y` (cast as `J`), and ...".

* **Direct dependent typing**:  We write `a :! A(x : I, y : J,...)` to mean:
  > `a` was declared directly in the type "`A` with the exact dependencies on `x` (cast as `I`), and `y` (cast as `J`), and ...".

* **"Direct dependency"**: Whenever we write `... :! A(x : I, y : J,...)` like in the previous item, then we implicitly also mean
  > `A` depends directly on `I, J, ...`

  _Note_. We do not introduce special notation for this in our type system, but in TypeQL this would be something like `A relates! I, relates! J, ...`.

* **Subtyping**: We write `A <= B` to mean:
  > Implicit casts from `A` to `B` are possible.

  _Example_ The `Child <= Person` means children `c` can cast into persons `c`.

* **Direct subtyping**: We write `A <! B` to mean:
  > An implicit cast from A to B was declared by user (we speak of a ***direct subtyping*** of A into B).

  _Example_: `Child <! Person`

  _Example_: `Child <! Nickname.Owner`

  _Example_: `Person <! Spouse`

* **Explicit castings**: We write `f : A -> B` to mean:
  > An explicity cast `f` from `A` to `B` is possible.

  _Example_ The `_val : Name -> string` means names `n` can be cast to string `_val(n)`.


### Conventions and notations

* **Dependency deduplication (+set notation)**:  Our type system rewrites dependencies by removing duplicates in the same trait, i.e. `a : A(x : I, y : I, y : I)` is rewritten to (and identified `a : A(x : I, y : I)`. In other words:
  > We **deduplicate** dependencies on the same element in the same trait.

  It is therefore convenient to use _set notation_, writing `a : A(x : I, y : I)` as `A : A({x,y}:I)`. (Similarly, when `I` appears `k` times in `A(...)`, we would write `{x_1, ..., x_k} : I`)
 
* **Trait specialization notation**:  If `A : KIND(J)`, `B : KIND(I)`, `A < B` and `J < I`, then we say:
  > The trait `J` of `A` **specializes** the trait `I` of `B`

  We write this as `A(J) < B(I)`. 

* **Role cardinality notation**: `|a|_I` counts elements in `{x_1,...,x_k} :I`

  _Example_: `m : Marriage({x,y} :Spouse)`. Then `|m|_{Spouse} = 2`.


### Algebraic type operators

We discuss the syntax for statements relating to operators and modalitites, and explain them in natural language statements.

* **Sum type**.  `A + B`. In words

    > The sum type of `A` and `B` contains all elements of `A` and all elements of `B`

* **Product type**. `(A, B)`. In words

    > The product type of `A` and `B` contains all elements of `A` and of `B`

* **Option type**. `A?`. In words

    > The option type of `A` contains all elements of `A` plus the empty element `()`

### List types

We discuss the syntax for statements relating to list types, and explain them in natural language statements.

* **List types**. For any `A : ALG`, we write `[A] : LIST` to mean
  > the type of `A`-lists, i.e. the type which contains lists `[a_0, a_1, ...]` of elements `a_i : A`.

    _Example_: Since `Person + City : ALG` we may consider `[Person + City] : LIST` — these are lists of persons or cities.

* **Dependency on list trait (of relation)**: For `A : REL`, we may have statements `A : REL([I])`. Thus, our type system has types of the form `A(x:[I]) : REL`.
    > `A(x:[I])` is a relation type with dependency on a list `x : [I]`.

    _Example_: `FlightPath : REL([Flight])`

* **Dependent list type (of attributes)**: For any `A : ATT(I)`, we introduce `[A] : LIST(I_{[]})` where `I_{[]}` is the **list version** of `I`. Thus, our type system has types of the form `[A](x:I) : LIST`.
    > `[A](x:I)` is a type of `A`-lists depending on trait `I`.

    _Example_: For `[Name] : LIST(Name.Owner)`, we may have attribute lists `[a,b,c] : [Name](x : Name.Owner)`

* **List length notation**: for list `l : [A]` the term `_len(l) : integer` represents `l`'s length.

_Remark for nerds: list types are not algebraic types... they are so-called inductive types!_

### Abstractness modality

The following is purely for keeping track of certain information in the type system.

* **Abstract modality**. Certain statements `J` about types (namely: type kinds `A : KIND(...)` and subtyping `A < B`) have _abstract_ versions written `#(J)`.

    > If `#(J)` is true then this means `J` is not true per se, but only _abstractly true_. Here, "abstractly true" is a special ("modal") truth-value which entails certain special behaviors in the database.

_Remark_: **Key**, **subkey**, **unique** could also be modalities, but adding them as such would add complexity to the type system language. We only add abstractness, because it's the most complicated language-wise. The rest is easy.

---

## THE RULES

This section describes the **rules** that govern the interaction of statements. This allows to **derive new statements** from existing statements. (We also state a few "invariants" which doesn't derive any new statements itself, but correlates derivable statements.)

### Types and subtypes

* **Direct typing rule**: From the statement `a :! A` the system can derive the statement `a : A`. (The converse is not true!)

  _Example_. `p :! Child` means the user has inserted `p` into the type `Child`. Our type system may derive `p : Person` from this (but _not_ `p :! Person`)

* **Direct dependent typing rule**: Similarly, from the statement `a :! A(x : I, y : J, ...)` the system can derive the statement `a : A(x : I, y : J, ...)`. (The converse is not true!)

* **Direct dependency invariant** (this is not a rule but a property of the type system): Whenever `a :! A(x : I)` and `B : KIND(I)` in the system then `B <= A` must also be true. In words: _`I` is a "direct dependency" of `A`_.

* **Subtyping rule**: If `A <= B` is true and `a : A`, then we can derive `a : B`.

* **Explicit casting rule**: If `f : A -> B` and `a : A`, then we can derive `f(a) : B` (where `f(a)` is syntax for _"`f` applied to `a`"_)

* **Direct subtyping rule**: From `A <! B` we can derive `A <= B`.

### Dependencies

We often give our rules for 1 or 2 dependencies, but they similarly apply for `k > 0` dependencies.

* **Applying type dependencies rule**: When `A : KIND(I,J)` and `x: I, y: J` then we can derive `A(x:I, y:J) : KIND` as a valid type in our system. In words:
  > We say `A(x:I)` is the type `A` with "applied dependency" `x : I`. In contrast, `A` by itself is an "unapplied" type.

* **Combining type dependencies rule**: Given `A : KIND(I)` and `A : KIND(J)` then derive `A : KIND(I,J)` is a valid dependent type. In words:
  > If a type separately depends on `I` and on `J`, then it may jointly depend on `I` and `J`! 

  _Example_: `HeteroMarriage : REL(Husband)` and `HeteroMarriage : REL(Wife)` then derive `HeteroMarriage : REL(Husband,Wife)`

* **Weakening type dependencies rule**: From `A : KIND(I,J)` we can derive `A : KIND(I)`. In words:
  > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

  _Example_: From `Marriage : REL(Spouse)` we can derive `Marriage : REL(Spouse)` and also `Marriage : REL` (we identify the empty brackets "`()`$" with no brackets).

* **Inheriting type dependencies rule**: If `A : KIND`, `B : KIND(I)`, `A <= B` and `A` has no trait strictly specializing `I` then derive `A : KIND(I)` ("strictly" meaning "not equal to `I`$"). In words:
  > Dependencies that are not specialized are inherited.

* **Projecting dependent terms rule**: If `a : A(x:I, y:J)` then derive `a : A(x:I)`. In other words:
    > Elements in `A(I,J)` casts into elements of `A(I)`.

    * _Example_: If `m : Marriage({x,y} :Spouse)` then both `m : Marriage(x:Spouse)` and `m : Marriage(y:Spouse)`

* **Upcasting dependent terms rule**: If `A(J) <= B(I)` (see "trait specialization" in syntax above) and `a : A(x:I)` then derive `a : B(x:J)`. In other words:
    > When `A` casts to `B`, and `I` to `J`, then `A(x : I)` casts to `B(x : J)`.

    _Example_: If `r : HeteroMarriage(x:Husband, y:Wife)` then `m : Marriage({x,y} :Spouse)`

The next rule is special to attributes, describing their interactions with value types.

* **Attribute identity rule**. If `V : VAL`, `A : ATT`, `_val : A -> V`, and `a, b :! A` such that `_val(a) =_val(b)` then derive `a = b` (which is syntax for equality)

### Algebraic type operators

* **Sum formation rule**: When `A : ALG, B : ALG` then `A + B : ALG`.Sum types, a.k.a. enum types, follow the usual rules in type systems. We don't spell them out.

    _Note_: the inclusion `A <= A + B` is also **subsumptive** subtyping. This induces certain equalities on elements: for example, if `A <= B`, then `A + B = B`. Indeed, when `a : A` then `a : B` and both include into `A + B` as the same element: `a = a : A + B` ... (therefore, technically `A + B` is the so-called _fibered_ sum over the intersection `A \cap B`). We omit the detailed rules here, and use common sense.

* **Product formation rule**: When `A : ALG, B : ALG` then `(A, B) : ALG`. Product types, a.k.a. tuple types, follow the usual rules in type systems. We don't spell them out.
    * We usually denote projection by postfix dot notation: `x : (A, B)` then `x.1 : A, x.2 : B` (similarly when fields are named in structs)

    _Note_: again, subsumptive subtyping interacts with this. For example, we would have if `A <= A'` and `B <= B'` then `(A, B) <= (A', B')` ...  We omit the detailed rules here, and use common sense.

* **Option type formation rule**: When `A : ALG` then `A? : ALG`. Option types follow the usual rules in type systems. We don't spell them out. (Effectively, options are sum types: `A? = A + { () }`)

    _Note_: same note as before applies. Also note `A?? = A?` due to subsumptive subtyping.

### List types

* **Direct typing list rule**: Given `l = [l_0,l_1,...] :! [A](x: I_{[]})` this implies `l_i :! A(x: I)`. In other words:
  > If the user intends a list typing `l :! [A]` then the list entries `l_i` will be direct elements of `A`.

* **Direct dependency list rule**: Given `l = [l_0,l_1,...] : [I]` and `a :! A(l : [I])` implies `a :! A({l_0, l_1, ...} : I)`. In other words:
  > If the user intends `a` to directly depend on the list `l` then they intend `a` to directly depend on each list's entries `l_i`.

* **Empty attribute lists**: For `A : ATT(I)` and `x : I_{[]}` such that no non-empty list `l : [A](x : I_{[]})` exists then this implies an empty **"default"** list `[] : [A](x : I_{[]})`.

_Note_. The last rule is the reason why we don't need the type `[A]?` in our type system — **the "None" case is simply the empty list.**

_Note 2_. List types also interact with subtyping in the obvious way: when `A <= B` then `[A] <= [B]`. We don't spell out the rules.

### Abstract modality

* _Key principle_: If `#(<statement>)` for some statement `<statement>` can be inferred in the type system, then we say:
  > "`<statement>` doesn't actually hold true, but it holds _abstractly_"

  * _Invariant_: In a commited schema, **it is never possible** that both `#(statement)` and `statement` are both true the same time (and _neither implies the other_).
  * _Remark_: The purposse of abstractness is always to *constrain `insert` behavior.*

* **Abstractly declared traits** `#(A <! I)` where `T : KIND(I)` means
  > `A` was declared to implement trait `I` abstractly.

  * _Direct-to-general_ When `#(A <! I)` then `#(A < I)` (the latter meaning "`A` implements trait `I` abstractly")
  * _Inheritance_: When `#(C <! I)`, `A < C`, and there is no `B` with `A <= B < C` and `B <! I` then `#(A < I)` (note that otherwise, by rules above, `A` inherits `A < I` from `B`)
  * _Un-ordering rule_: When `A < B[].O` then `#(A < B.O)`
 
* **Abstract roles** `#(A : REL(I))` means:
  > Relation `A` depends on role type `I` abstractly
 
  * _Un-specialization rule_: When `A : REL(I)`, `I < J`, (i.e., `A relates I as J`) then `#(A : REL(J[]))`
  * _Un-specialization rule (list case)_: When `A : REL(I[])`, `I < J`, (i.e., `A relates I[] as J[]`) then `#(A : REL(J[]))`
  * _Un-ordering rule_: When `A : REL(I[])` then automatically `#(A : REL(I))`

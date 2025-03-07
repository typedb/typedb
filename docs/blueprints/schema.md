---
status: complete
---

# Data definition language

Specification of how to modify database schema. On this page:

* **System invariants**
* **Define**
* **Undefine**
* **Redefine**
* **Labels and aliases**


## System invariants (a.k.a. "schema principles")

Invariants hold true in any valid database system state. We usually express the invariants below in terms of **TypeQL statements** but sometimes it more convenient to direct use **type system statements**. See the [read spec](./read.md) for how data read statements **unpack** into [type system](type_system.md) statements (or see the [dictionary here](type_system.md) for a short summary).

> If it has keywords it's **TypeQL**, if it has symbols it **type system** statements.

### Core invariants

1. When `A sub! B` matches then either:
    * `_kind(A) = _kind(B)`
    * `A : ENT + REL`, `B : Trait`, 
1. Given ERA type `A`, `A sub! B` matches for at most one `B`
1. Given role `I`, `A relates! I` matches for at most one `A`
1. Given relation `A`, either `A @abstract` matches or `A relates! I` matches for at least one `I`.
1. Given attribute `A`, either `A @abstract` matches or `A value V` matches for exactly one `V`.
1. 🔶 `A relates I` and `B relates J[]` never both match _non-abstractly_ at the same time when `A, B` and `I, J` are in same hierarchy respectively (see Rmk. below)
1. 🔶 `A owns M` and `B owns N[]` never both match _non-abstractly_ at the same time when `A, B` and `M, N` are in same hierarchy respectively.

_Remark_. By "matching a `<statement>` non-abstractly" in TypeQL we mean `<statement>; not { <statement> @abstract; };`

### Abstractness invariants

_Note_: **Abstract traits** and **abstract roles** come with several systematic rules. They are therefore most elegantly understood as part of the type system ("abstract statements" in the type system are written using `#(...)`). In contrast, **abstract types** (and also other modalities, like key attributes, uniqueness, dinstinctness, etc.) are pretty "simple" and are not made part of the type system—we simply record them "in the schema".

1. `<statement>` and `#(<statement>)` ***never both true*** at the same time in the [type system](type_system.md)
1. `A sub B` and `kind A @abstract` then must have `kind B @abstract`
1. `A(I) <= B(J)` and `A relates I @abstract` then cannot have `B relates J` non-abstractly
1. `A(I) <= B(J)` and `C plays A:I @abstract` then cannot have `C plays B:J` non-abstractly
1. `M <= N` and `A owns M @abstract` then cannot have `A owns N` non-abstractly

## Define

### Overview

* Only **adds** [axioms](./type_system.md) to the type system, with one exception:
  * some `@abstract` definitions may overwrite previous non-abstract definitions
* Takes **multiple** statements
* **Order** of statements never matters
  * abstract version of statements overwrite non-abstract versions

### Types

* `entity A` adds `A : ENT`
* `(entity) A sub B` adds `A : ENT, A <! B`

* `relation A` adds `A : REL`
* `(relation) A sub B` adds `A : REL, A <! B` where `B : REL` 
* `(relation) A relates I` adds `A : REL(I)` and `I : TRAIT`.
* `(relation) A relates I as J` adds `A : REL(I)`, `I <! J` where `B : REL(J)` and `A <! B`
* 🔶 `(relation) A relates I[]` adds `A : REL([I])`
* 🔶 `(relation) A relates I[] as J[]` adds `A : REL([I])`, `I <! J` where `B : REL([J])` and `A <! B`

* `attribute A` adds both:
  * `A : ATT(A.O)` and `A.O : TRAIT`
  * `[A] : LIST(A[].O)` and `A[].O : TRAIT`
* `(attribute) A sub B` adds both:
  * `A : ATT(A.O)`, `A <! B` and `A.O <! B.O` where `B : ATT(B.O)`
  * `[A] : LIST(A[].O)`, `[A] <! [B]` and `A[].O <! B[].O` where `B : ATT(B.O)`
* `(attribute) A value V` adds `_val : A -> V` where `V : VAL`

* `A plays B:I` adds `A <! I` where `B relates! I`.

* `A owns B` adds `A <! B.O` where `B: ATT(B.O)`, `A :OBJ`
* 🔶 `A owns B[]` adds `A <! B[].O` where `B: ATT(B.O)`, `A :OBJ`

### Abstractness

Abstractness adds additional invariants to our system, **but it also adds** abstractly true axioms to our type system.

* `(kind) A @abstract` adds ***nothing*** (we just remember schema definition), but enforces
  * **invariant**: no `a :! A(...)` can exist

  _Note_:  abstractness for types themselves is not directly recorded in [type system](type_system.md) ... its rules are simple enough to deal with it "verbally".
* `A relates I @abstract` adds `#(A : REL(I))` and enforces
  * **invariant**: no `a :! A(... : I, ...)` can exist
* 🔶 `A relates I[] @abstract` adds `#(A : REL([I]))` and enforces
  * **invariant**: no `a :! A(... : I[], ...)` can exist
* `A plays B:I @abstract` adds `#(A <! I)` and enforces
  * if `B : REL(I)` then **invariant**: no `... :! B(a : I, ...)` can exist
  * 🔶if `B : REL(I[])` then **invariant**: no `... :! B(a : I[], ...)` can exist
* `A owns B @abstract` adds `#(A <! B.O)` and enforces
  * **invariant**: no `... :! B(a : B.O)` can exist
* 🔶 `A owns B[] @abstract` adds `#(A <! B[].O)` and enforces
  * **invariant**: no `... :! B[](a : B.O)` can exist

### Constraints

Constraints add additional invariants to our system. 

* `A relates I @card(n..m)` 
  * **defaults** to `@card(1..1)` if omitted ("one")
  * **invariant**: `n <= k <= m` whenever `a :! A'({x_1, ..., x_k} : I)`, `A' <= A`,
* `A plays B:I @card(n..m)`
  * **defaults** to `@card(0..)` if omitted ("many")
  * **invariant**: `n <= _size(B(a:I)) <= m` for all `a : A`
* `A owns B @card(n...m)`
  * **defaults** to `@card(0..1)` if omitted ("one or null")
  * **invariant**: `n <= _size(B(a:I)) <= m` for all `a : A`

* 🔶 `A relates I[] @card(n..m)`
  * **defaults** to `@card(0..)` if omitted ("many")
  * **invariant**: `n <= _len(l) <= m` whenever `a :! A'(l : [I])`, `A' <= A`, `A' : REL([I])`
* 🔶 `A owns B[] @card(n...m)`
  * **defaults** to `@card(0..)` if omitted ("many")
  * **invariant**: `n <= _len(l) <= m` whenever `l : [B](a:B.O)` for `a : A`

_Remarks_. (1) Upper bounds can be omitted, writing `@card(2..)`, to allow for arbitrary large cardinalities. (2) When we have direct subtraits `I_i <! J`, for `i = 1,...,n`, and each `I_i` has `card(n_i..m_i)` while J has `card(n..m)` then we must have `sum(n_i) <= m` and `n <= sum(m_i)` for the schema to be usable.

* `A owns B @unique`
  * **invariant**: if `b : B(a:B.O)` for some `a : A` then this `a` is unique (for fixed `b`).
* `A owns B @key`
  * **invariant**: if `b : B(a:B.O)` for some `a : A` then this `a` is unique, and also `|B(a:B.O)| = 1`.
* 🔶 `A owns B1 @subkey(<LABEL>); A owns B2 @subkey(<LABEL>)`
  * **invariant**: if `b_1 : B_1(a:B_1.O), b_2 : B_2(a:B_2.O)` for some `a : A` then this `a` is unique for the given tuple `(b_1, b_2)`, and also `_size((B_1(a:B_1.O), B_2(a:B_2.O))) = 1`.
  * this **generalizes** to `n` subkeys.
* 🔶 `A owns B[] @distinct`
  * **invariant**:  when `[b_1, ..., b_n] : [B]` then all `b_i` are distinct.
* 🔶 `B relates I[] @distinct`
  * **invariant**:  when `[x_1, ..., x_n] : [I]` then all `x_i` are distinct.

* `A owns B @values(v1, v2)`
  * **invariant**:  if `a : A` then `_val(a) in {v_1, v_2}` , where `A : ATT`, `_val : A -> V`, `v_i : V`,
* 🔶 `A owns B[] @values(v1, v2)`
  * **invariant**:  if `l : [A]` and `a in l` then `a in {v_1, v_2}` , where `A : ATT`, `_val : A -> V`, `v_i : V`,
  * this **generalizes** to `n` values.
* `A owns B @regex(<REGEX>)`
  * **invariant**:  if `a : A` then `a` conforms with regex `<EXPR>`.
* 🔶 `A owns B[] @regex(<REGEX>)`
  * **invariant**:  ... (similar, but for individual list members)
* `A owns B @range(v1..v2)`
  * **invariant**:  if `a : A` then `a in [v_1,v_2]` (conditions as before).
* 🔶 `A owns B[] @range(v1..v2)`
  * **invariant**:  ... (similar, but for individual list members)

* `A value B @values(v1, v2)`
  * **invariant**: if `a : A` then `_val(a) in {v_1, v_2}` , where:
    * either `A : ATT`, `_val : A -> V`, `v_i : V`,
    * or `A` is the component of a struct, see section on struct defs.
  * this **generalizes** to `n` values.
* `A value B @regex(<REGEX>)`
  * **invariant**: if `a : A` then `a` conforms with regex `<REGEX>`, where:
    * either `A : ATT`, `_val : A -> V`,
    * or `A` is the component of a struct, see section on struct defs.
* `A value B @range(v1..v2)`
  * **invariant**: if `a : A` then `a in [v_1,v_2]` (conditions as before), where:
    * either `A : ATT`, `_val : A -> V`,
    * or `A` is the component of a struct, see section on struct defs.

### Structs and functions

* 🔶 **Struct** definition takes the form: 
    ```
    struct S:
      C1 value V1?,
      C2 value V2;
    ```
    which adds
    * a _struct label_: `S : VAL`
    * with _struct identity_:  `S = (V1?, V2)` (the latter is type construction, see [type system spec](type_system.md))
    * _Projections_ (as usual for product types): when `s : S` then `s.C1 : V1?` and `s.C2 : V2?`
    * ... all this **generalizes** to n fields in the struct

* **Function** definition takes the form:
    ```
    struct ;
      <read-pipeline>
    ```
  See [functions spec](functions.md) for details

## Undefine semantics

### Overview

* Only **removes** [axioms](./type_system.md) to the type system, with one exception:
  * some `@abstract` undefinitions may add non-abstract definitions
* Takes **multiple** statements
* **Order** of statements never matters
* Can be a **no-op**


### Types

* `entity A` removes `A : ENT`
* `sub B from (entity) A` removes `A <! B`

* `relation A` removes `A : REL`
* `sub B from (relation) A` removes `A <= B`
* `relates I from (relation) A` removes `A : REL(I)`
* `as J from (relation) A relates I` removes `I <! J` 
* 🔶 `relates I[] from (relation) A` removes `A : REL([I])$
* 🔶 `as J[] from (relation) A relates I[]` removes `I <! J`

* `attribute A` removes `A : ATT` and `A : ATT(A.O)`
* `value V from (attribute) A value V` removes `_val : A -> V`
* `sub B from (attribute) A` removes `A <! B` and `A.O <! B.O`

* `plays B:I from (kind) A` removes `A <! I` 

* `owns B from (kind) A` removes `A <! B.O` 
* 🔶 `owns B[] from (kind) A` removes `A <! B[].O`

### Constraints

_In each case, `undefine` removes the additional invariant (minor exception is `subkey`, which _changes_ the invariant by shrinking the domain of the composite key).

* `@card(n..m) from A relates I`
* `@card(n..m) from A plays B:I`
* `@card(n...m) from A owns B`

* 🔶 `@card(n..m) from A relates I[]`
* 🔶 `@card(n...m) from A owns B[]`

* `@unique from A owns B`
* `@key from A owns B`
* `@subkey(<LABEL>) from A owns B` removes `B` as part of the `<LABEL>` key of `A`

* 🔶 `@distinct from A owns B[]`
* 🔶 `@distinct from B relates I[]`

* `@values(v1, v2) from A owns B` 
* `@range(v1..v2) from A owns B`

* `@values(v1, v2) from A value B` 
* `@range(v1..v2) from A value B`

### Abtractness

* `@abstract from (kind) B`
* `@abstract from A plays B:I`
* `@abstract from A owns B`
* `@abstract from A owns B[]`
* `@abstract from A relates I`
* 🔶 `@abstract from A relates I[]`

### Structs and fucntions

* 🔶 `struct S;` removes `S : TYPE` if possible (i.e. not used in other definitions)
* `fun F;` removes function if possible (i.e. not used in other definitions)

## Redefine 

### Overview

* Only **replaces** [axioms](./type_system.md) to the type system, with one exception:
* Takes **a single** statement
* Cannot be a **no-op**

### Type axioms

* **cannot** redefine `entity A`
* `(entity) A sub B` redefines `A <= B`
* **cannot** redefine `relation A` 
* `(relation) A sub B` redefines `A <= B`, ***requiring*** 
  * either `A <! B' != B` (to be redefined)
  * or `A` has no direct super-type
* 🔶 `(relation) A relates I` redefines `A : REL(I)`, ***requiring*** that `A : REL([I])` (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(0..)`) 
  * _Data transformation_: moves any `a : A(l : [I])` with `l = [l_0, l_1, ..., l_{k-1}]` to `a : A({l_0,l_1,...,l_{k-1}} : I`
* `(relation) A relates I as J` redefines `I <! J`, ***requiring*** that either `I <! J' != J` or `I` has no direct super-role
* 🔶 `(relation) A relates I[]` redefines `A : REL([I])`, ***requiring*** that `A : REL(I)` (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(1..1)`) (STICKY)
  * _Data transformation_: moves any `a : A(l : [I])` with `l = [l_0, l_1, ..., l_{k-1}]` to `a : A({l_0,l_1,...,l_{k-1}} : I`
* 🔶 `(relation) A relates I[] as J[]` redefines `I <! J`, ***requiring*** that either `I <! J' != J` or `I` has no direct super-role

#### **Case ATT_REDEF**
* **cannot** redefine `attribute A`
* `(attribute) A value V` redefines `_val : A -> V`
* **cannot** redefine `(attribute) A sub B`

#### **Case PLAYS_REDEF**
* **cannot** redefine `(kind) A plays B:I`

#### **Case OWNS_REDEF**
* **cannot** redefine `(kind) A owns B`
* **cannot** redefine `(kind) A owns B[]`

### Constraints

_In each case, `redefine` redefines the postulated condition._

* `A relates I @card(n..m)`
*  `A plays B:I @card(n..m)`
* `A owns B @card(n...m)`
* `A relates I[] @card(n..m)`
* `A owns B[] @card(n...m)`
* `A owns B @values(v1, v2)`
* `A owns B @regex(<EXPR>)`
* `A owns B @range(v1..v2)`
* `A value B @values(v1, v2)`
* `A value B @regex(<EXPR>)`
* `A value B @range(v1..v2)`

**Cannot** redefine `@unique`, `@key`, `@abstract`, or `@distinct`.

### Structs and functions

* 🔶 `redefine struct A ...` replaces the previous definition of `A` with a new one. 
* 🔶 `redefine fun f ...` replaces the previous definition of `f` with a new one. 

## Labels and aliases

### Overview

* Each type has a **label**, which is its primary identifier
* In addition, the user may _define_ (and _undefine_) any number of aliases, which can be used in place of the primary label in pattern
* Primary labels themselves can be _redefined_
* Labels and aliases must be unique (e.g. one type's label cannot be another type's alias)

### Define

* **adding aliases**
```
define person alias p, q, r;
define marriage:spouse alias marriage:p, marriage:q, marriage:r;
```

### Undefine

* **removing aliases**
```
undefine alias p, q, r from person;
undefine alias marriage:p, marriage:q, marriage:r from marriage:spouse;
```

### Redefine 

* **changing primary label**
```
redefine person label animal;
redefine marriage:spouse label marriage:super_spouse;
```



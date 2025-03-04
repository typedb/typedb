---
status: complete
---

# Writing data

Specification of how to write data to database in read-write pipelines. On this page:

_**Query PROCESSING**_

1. **Variable categorization**: this is exactly as in [read spec](read.md) but extended to patterns in write stages
2. **Type check**: this is exactly as in [read spec](read.md) but extended to patterns in write stages

_**Query OPERATION**_

1. **Insert** operation
2. **Delete** operation
2. **Update** operation
2. **Put** operation

_**Answer FORMATTING**_

1. **Fetch** JSON: this is exactly as in [read spec](read.md) but extended to patterns in write stages

# PROCESSING

See [read spec](read.md), nothing to add (for now).

# EVALUATION

## Insert

### Overview

* adds new axioms to type system (potentially replacing old ones). which may affect:
    * terms in types (which also **get added to concept row**)
    * dependencies between terms
* multiple statements per stage
* order does not (and must not) matter
* execute try blocks exactly when all variable in block are non-empty

### Statements

#### Term inserting

These statement **produce** their primary term variable (they **require** all other variables).

* `$x isa $T` adds new `a :! $T @ r`, `$T @ r : ERA`, and produces `$x @ r = a` in output row
* `$x isa $T ($I: $y, ...)` adds new `a :! $T($y : $I, ...) @ r`, `$T @ r : REL`, and produces `$x @ r = a` in output row
* `$x isa $T ($I[]: $y, ...)` adds new `a :! $T($y : $I[], ...) @ r`, `$T @ r : REL`, and produces `$x @ r = a` in output row
 
    _Note_. When new relations with list dependencies are inserted, their **list dependencies are always populated if the empty list** if not otherwise specified.
* `$x isa $T EXPR` adds new `a :! $T @ r`, `$T @ r : ATT` and `_val(a) = EXPR @ r`, and produces `$x @ r = a` in output row (**reject** if `$T @ r` is not independent) 
* `$T` is equivalent to `$_ isa $T`
* `$T ($R: $y, ...)`  is equivalent to `$_ isa $T ($R: $y, ...)`
* `$T EXPR` is equivalent to `$_ isa $T EXPR`

_**Note**_ The usual schema-consistency checks should be applied of course.

#### Dependency inserting

These statement **require** their variables.

* `$x links ($I : $l)` replaces `$x @ r :! A(j : J, ...) @ r` by `$x @ r :! A($l : $I, ...) @ r`
* 🔶 `$x links ($I[] : $l)` replaces `$x @ r :! A(j : $I[], ...) @ r` by `$x @ r :! A($l : $I[], ...) @ r` 
* 🔶 `$x links ($I[] : EXPR)` replaces `$x @ r :! A(j : $I[], ...) @ r` by `$x @ r :! A(EXPR : $I[], ...) @ r`

    _Note_: List dependencies are always present (empty list acts as "absence" of dependency).

* `$x has $A $y`, for `_cat($y) = Inst_` adds new `a :! $A @ r($x @ r : $A.O @ r)` and add the new cast `_val(a) = _val(y)` (**reject** if `_type($y) != $A @ r`)
* `$x has $A EXPR` adds new element `a :! $A($x : $A.O) @ r` and add cast `_val(a) = EXPR @ r`

    _Note_: By attribute identity rule (see [type system spec](type_system.md)) inserting an attribute with same type and value twice will be idempotent.

* 🔶 `$x has $A[] EXPR` adds new list `l = [l_1, l_2, ...] :! $A[]($x : A[].O}) @ r` **and** new attributes `l_i :! $A ($x : $A.O) @ r` where
    * denote by `[v_1,v_2, ...] = EXPR @ r` the evaluation of the list expression (by substituting to input row `r`)
    * the list `l` has the same length as `[x_1,x_2, ...] = EXPR @ r` and we construct its element values as follows:
        * if `x_i` is an attribute instance, we add new cast `_val(l_i) = _val(v_i)` (**reject** if `_type(x_i) != $A @ r`)
        * if `x_i` is a value, we add new cast `_val(l_i) = x_i`

#### Optional inserts

* `try { S_1; S_2; ... }` where `S_i` are insert statements as described above: execute this if all required variables are available. 

## Delete

### Overview

* removes axioms from type system, namely
  * terms in types (which also **gets removed from concept row**)
  * dependencies between terms
* multiple statements per stage
* order does not (and must not) matter
* execute try blocks exactly when all variable in block are non-empty

### Statements

All variables in delete statements are **required**.

#### Term deletes

* `$x;` removes `$x @ r :! A(...)`. If `$x @ r` is an object, we also:
    * replace any `b :! B($x @ r : I, z : J, ...)` by `b :! B(z : J, ...)` for all such dependencies on `$x @ r`

    _Note_. This applies both to `B : REL` and (**independent**) `B : ATT`.

### Dependency deletes

* `($I: $y) of $x` replaces `$x @ r :! $A($y : $I, z : J, ...) @ r` by `$x @ r :! $A (z : J, ...) @ r`
* 🔶 `($I[]: $l) of $x` replaces `$x @ r :! $A ($l : $I[], z : J, ...) @ r` by `$x @ r :! $A(z : J, ...)`

* `$A $y of $x` removes any `$y @ r :! $A($x : $A.O) @ r`
    * `$A of $x` removes any `$... :! $A($x : $A.O) @ r` 
* 🔶 `$A[] $y of $x` removes any `$y @ r :! $A[]($x : $A[].O) @ r`
    * 🔶 `$A[] of $x` removes any `$... :! $A[]($x : $A[].O) @ r`

#### Optional deletes

* `try { S_1; S_2; ... }` where `S_i` are delete statements as described above: execute this if all required variables are available.

#### Cascading clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Update

* removes axioms from type system, namely
    * terms in types (which also **gets removed from concept row**)
    * dependencies between terms
* **one statement** per stage (to avoid execution ordering issues)
* execute try blocks exactly when all variable in block are non-empty

### Overview

### Update statements

Statements **require** all their variables. We also need all cardinalities of affected (non-list) roles and attributes to be **between 0..1**.

* `$x links ($I: $y);` updates `$x @ r :! A(z : $I, ...)` or `$x @ r :! A(...)` to `$x @ r :! A($x : $I, ...) @ r`
* `$x links ($I[]: $y);` updates `$x @ r :! A(z : $I[], ...)` to `$x @ r :! A($x : $I, ...) @ r`

    _Note_. Lists are always present (empty list _is_ "absent list").

* `$x has $B $y;`, where `_cat($y) = Inst_`, updates `b :! $B($x : $B.O) @ r` to `$y @ r :! $B(x : $B.O) @ r` **or** adds the latter. (**reject** if `_type($y)` is not exactly `$B @ r`)
* `$x has $B EXPR;` updates `b :! $B($x : $B.O) @ r` to `$y @ r :! $B(x : $B.O) @ r` **or** adds the latter.
* `$x has $B[] EXPR;` updates `b :! $B[]($x : $B[].O) @ r` to `EXPR @ r :! $B[](x : $B[].O) @ r` **or** adds the latter.
  * ... **reject** if `EXPR @ r` has an instance element of type not exactly equal to `$B @ r`

#### Optional updates

* `try { S_1; S_2; ... }` where `S_i` are update statements as described above: execute this if all required variables are available.

## Put

`put <PUT>` is equivalent to
```
if (match <PUT>; check;) then (match <PUT>;) else (insert <PUT>)
```
In particular, `<PUT>` needs to be an `insert` compatible set of statements.

# FORMATTING

See [read spec](read.md), nothing to add (for now).

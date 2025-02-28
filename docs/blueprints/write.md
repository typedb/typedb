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

### Statements

## Delete

### Overview

### Statements

## Update

### Overview

### Statements

## Put

### Overview

### Statements

# FORMATTING

See [read spec](read.md), nothing to add (for now).

---
OLD

### Basics of inserting

An `insert` clause comprises collection of _insert statements_

* _Input crow_: The clause can take as input a stream `{ r }` of concept rows `r`, in which case
    * the clause is **executed** for each row `r` in the stream individually

* _Extending input row_: Insert clauses can extend bindings of the input concept row `r` in two ways
    * `$x` is the subject of an `isa` statement in the `insert` clause, in which case `r(x) =` _newly-inserted-concept_ (see "Case **ISA_INS**")
    * `$x` is the subject of an `let` assignment statement in the `insert` clause, in which case `r(x) =` _assigned-value_ (see "Case **LET_INS**")

#### (Theory) Execution

_Execution_: An `insert` clause is executed by executing its statements individually.
* Not all statement need to execute (see Optionality below)
    * **runnable** statements will be executed
    * **skipped** statements will not be executed
* The order of execution is arbitrary except for:
    1. We always execute all `let` assignments before the runnable statements that use the assigned variables variables. (There is an acyclicity condition that guarantees this is possible).
* Executions of statements will modify the database state by
    * adding elements
    * refining dependencies
* (Execution can also affect the state of concept row `r` as mentioned above)
* Modification are buffered in transaction (see "Transactions")
* Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) Optionality

* 🔶 **Optionality**: Optional variables are those exclusively appearing in a `try` block
    * `try` blocks in `insert` clauses cannot be nested
    * variables in `try` are variables are said to be **determined** if
        * they are determined outside the block, i.e.:
            * they are assigned in the crow `r`
            * they are subjects of `isa` or `let` statements outside the block
        * they are subjects of `isa` or `let` statements inside the block
    * If any variable in the `try` block is _not_ determined, then `try` block statement is **skipped** (i.e. **not executed**)
    * If all variables are , the `try` block `isa` statements are marked **runnable**.
    * All variables outside of a `try` block must be bound outside of that try block (in other words, variable in a block bound with `isa` cannot be used outside of the block)

### Insert statements

#### **Case LET_INS**
* ➖`let $x = <EXPR>` adds nothing to the DB, and but sets `r(x) = v_r(expr)` in the in put crow

_System property_:

1. ➖`$x` cannot be insert-bound elsewhere (i.e. no other `isa` or `let`)
2. ➖_Acyclicity_ All `isa` or `let` statements must be acyclic. For example we cannot have:
    ```
    insert
      let $x = $y;
      $y isa name $x;
    ```
3. ➖_No reading_ `<EXPR>` cannot contain function calls.

_Note_. All **EXPR_INS** statements are executed first as described in the previous section.

#### **Case ISA_INS**
* ➖ `$x isa $T` adds new `a :! r(T)`, `r(T) : ERA`, and sets `r(x) = a`
* 🔶 `$x isa $T ($R: $y, ...)` adds new `a :! r(T)(r(y):r(R))`, `r(T) : REL`, and sets `r(x) = a`
* 🔶 `$x isa $T <EXPR>` adds new `a :! r(T)`, `r(T) : ATT`, and sets `r(x) = a` and `_val(a) = v_r(expr)`

_System property_:

1. ➖ `$x` cannot be bound elsewhere (i.e. `$x` cannot be bound in the input row `r` nor in other `isa` or `let` statements).
1. 🔮 `<EXPR>` must be of the right value type, and be evaluatable (i.e. all vars are bound).
1. 🔶 In the last case, `r(T)` must be an independent attribute, i.e. the schema must contain `attribute r(T) (sub B) @indepedent`

#### **Case ANON_ISA_INS**

* 🔶 `$T` is equivalent to `$_ isa $T`
* 🔶 `$T ($R: $y, ...)`  is equivalent to `$_ isa $T ($R: $y, ...)`
* 🔶 `$T <EXPR>` is equivalent to `$_ isa $T <EXPR>`

#### **Case LINKS_INS**

* ➖ `$x links ($I: $y)` replaces `r(x) :! A(a : J, b : K, ...)` by `r(x) :! A(r(y)a : r(I), b : K, ...)`

_Remark_. Set semantics for traits means that inserts become idempotent when inserting the same role players twice.

_System property_:

1. ➖ _Capability check_.
    * Must have `T(x) <= B : REL(r(I))` **non-abstractly**, i.e. `# (B : REL(r(I)))` is not true for the minimal choice of `B` satisfying the former
    * Must have `T(y) <= B <! r(I)` **non-abstractly**, i.e. `# (B <! r(I))` is not true for the minimal `B` satisfying the former.

#### **Case LINKS_LIST_INS**
* 🔶 `$x links ($I[]: <T_LIST>)` replaces `r(x) :! A()` by `r(x) :! A(l : [r(I)])` for `<T_LIST>` evaluating to `l = [l_0, l_1, ...]`

_System property_:

1. 🔶 _System cardinality bound: **1 list per relation per role**_. Transaction will fail if `r(x) :! A(...)` already has a roleplayer list. (In this case, user should `update` instead!)
1. 🔶 _Capability check_.
    * Must have `T(x) <= B : REL(r(I))` **non-abstractly**, i.e. `# (B : REL(r(I)))` is not true for the minimal choice of `B` satisfying the former
    * Must have `l_i : T_i <= B <! r(I)` **non-abstractly**, i.e. `# (B <! r(I))` is not true for the minimal `B` satisfying the former.

#### **Case HAS_INS**
* ➖ `$x has $A $y` adds new `a :! r(A)(r(x) : r(A).O)` and
    * If `$y` is instance add the new cast `_val(a) = _val(y)`
    * If `$y` is value var set `_val(a) = r(a)`
* ➖`$x has $A == <VAL_EXPR>` adds new element `a :! r(A)(r(x) : r(A).O)` and add cast `_val(a) = v_r(val\_expr)`
* ➖`$x has $A <NV_VAL_EXPR>` is shorthand for `$x has $A == <NV_VAL_EXPR>` (recall `NV_VAL_EXPR` is an expression that's not a sole variable)

_System property_:

1. ➖ _Idempotency_. If `a :! r(A)` with `_val(a) = _val(b)` then we equate `a = b` (this actually follows from the "Attribute identity rule", see "Type system").
1. 🔶 _Capability check_. Must have `T(x) <= B <! r(A).O` **non-abstractly**, i.e. `# (B <! r(A).O)` is not true for the minimal choice of `B` satisfying  `T(x) <= B <! r(A).O`
1. 🔶 _Type check_. Value `v` of newly inserted attribute must be of right value type (up to implicit casts, see "Expression evaluation"). Also must have `T(y) <= r(A)` if `y` is given.

_Remark_: ⛔ Previously we had the constraint that we cannot add `r(y) :! A(r(x) : A.O)` if there exists any subtype `B < A`.

#### **Case HAS_LIST_INS**
* 🔶 `$x has $A[] == <LIST_EXPR>` adds new list `l = [l_1, l_2, ...] :! [r(A)](r(x) : A[].O})` **and** new attributes `l_i :! r(A)(r(x) : r(A).O)` where
    * denote by `[v_1,v_2, ...] = v_r(list\_expr)` the evaluation of the list expression (relative to crow `r`)
    * the list `l` has the same length as `[x_1,x_2, ...] = v_r(list\_expr)`
        * if `y_i = x_i` is an attribute instance, we add new cast `_val(l_i) = _val(v_i)`
        * if `v_i = x_i` is a value, we add new cast `_val(l_i) = v_i`
* 🔶 `$x has $A[] <NV_LIST_EXPR>` is shorthand for `$x has $A == <NV_LIST_EXPR>` (recall `NV_LIST_EXPR` is an expression that's not a sole variable)
* 🔶 `$x has $A[] $y` is shorthand for `$x has $A == $y` but infers that the type of `$y` is a subtype of `[r(A)]` (and thus we must have `y_i = x_i` for all `i` above)

_System property_:

1. ➖ _Idempotency_. Idempotency is automatic for (since lists are identified by their list elements) and enforced for new attributes as before.
1. 🔶 _System cardinality bound: **1 list per owner**_. We cannot have any `k : [r(A)](r(x) : A[].O})` with `k != l`. (Users should use "Update" instead!)
1. 🔶 _Capability check_. Must have `T(x) <= B <! r(A)[].O` **non-abstractly**, i.e. `# (B <! r(A)[].O)` is not true for the minimal choice of `B` satisfying `T(x) <= B <! r(A)[].O`
1. 🔶 _Type check_. For each list element, must have either `T(v_i) : V` (up implicit casts) or `T(y) <= r(A)`.

### Optional inserts

#### **Case TRY_INS**
* 🔶 `try { <INS>; ...; <INS>; }` where `<INS>` are insert statements as described above.
    * `<TRY_INS>` blocks can appear alongside other insert statements in an `insert` clause
    * Execution is as described in "Basics of inserting" (**#BDD**)


## Delete semantics

### Basics of deleting


A `delete` clause comprises collection of _delete statements_.

* _Input crows_: The clause can take as input a stream `{ r }` of concept rows `r`:
    * the clause is **executed** for each row `r` in the stream individually

* _Updating input rows_: Delete clauses can update bindings of their input concept row `r`
    * Executing `delete $x;` will remove `$x` from `r` (but `$x` may still appear in other crows `r'` of the input stream)

_Remark_: Previously, it was suggested: if `$x` is in `r` and `r(x)` is deleted from `T_r(x)` by the end of the execution of the clause (for _all_ input rows of the input stream) then we set `r(x) = ()` and `T_r(x) = ()`.
Fundamental question: **is it better to silently remove vars? Or throw an error if vars pointing to deleted concepts are used?** (STICKY)
* Only for `delete $x;` can we statically say that `$x` must not be re-used
* Other question: would this interact with try? idea: take `r(x) = ()` if it points to a previously deleted concept

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

#### (Theory) execution

* _Execution_: An `delete` clause is executed by executing its statements individually.
    * Not all statement need to execute (see Optionality below)
        * **runnable** statements will be executed
        * **skipped** statements will not be executed
    * The order of execution is arbitrary order.
    * Executions of statements will modify the database state by
        * removing elements
        * remove dependencies
    * Modifications are buffered in transaction (see "Transactions")
    * Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) optionality

* 🔶 _Optionality_: Optional variables are those exclusively appearing in a `try` block
    * `try` blocks in `delete` clauses cannot be nested
    * `try` blocks variables are **determined* if they are bound in `r`
    * If any variable is not determined, the `try` block statements are **skipped**.
    * If all variables are determined, the `try` block statements are **runnable**.

### Delete statements

#### **Case CONCEPT_DEL**
* ➖ `$x;` removes `r(x) :! A(...)`. If `r(x)` is an object, we also:
    * replaces any `b :! B(r(x) : I, z : J, ...)` by `b :! B(z : J, ...)` for all such dependencies on `r(x)`

_Remark 1_. This applies both to `B : REL` and `B : ATT`.

_Remark 2_. The resulting `r(x) :! r(A)(z : J, ...)` must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

_System property_:

1. ➖If `r(x) : A : ATT` and `A` is _not_ marked `@independent` then the transaction will fail.


#### **Modifier: CASCADE_DEL**
* 🔮 `delete` clause keyword can be modified with a `@cascade(<LABEL>,...)` annotation, which acts as follows:

  If `@cascade(C, D, ...)` is specified, and `$x` is delete then we not only remove `r(x) :! A(...)` but (assuming `r(x)` is an object) we also:
    * whenever we replace `b :! B(r(x) : I, z : J, ...)` by `b :! B(z : J, ...)` and the following are _both_ satisfied:

        1. the new axiom `b :! B(...)` violates trait cardinality of `B`,
        2. `B` is among the listed types `C, D, ...`

      then delete `b` and _its_ depenencies (the cascade may recurse).

_Remark_. In an earlier version of the spec, condition (1.) for the recursive delete was omitted—however, there are two good reasons to include it:

1. The extra condition only makes a difference when non-default trait cardinalities are imposed, in which case it is arguably useful to adhere to those custom constraints.
2. The extra condition ensure that deletes cannot interfere with one another, i.e. the order of deletion does not matter.

#### **Case ROL_OF_DEL**
* ➖ `($I: $y) of $x` replaces `r(x) :! r(A)(r(y) : r(I), z : J, ...)` by `r(x) :! r(A)(z : J, ...)`

_Remark_. The resulting `r(x) :! r(A)(z : J, ...)` must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

#### **Case ROL_LIST_OF_DEL**
* 🔶 `($I[]: <T_LIST>) of $x` replaces `r(x) :! r(A)(l : r(I))` by `r(x) :! r(A)()` for `l` being the evaluation of `T_LIST`.

#### **Case ATT_OF_DEL**
* 🔶 `$y of $x` replaces any `r(y) :! T(y)(r(x) : r(B).O)` by `r(y) :! T(y)()`
* ➖`$B of $x` replaces any `a :! r(B)(r(x) : r(B).O)` by `a :! r(B)()`

_System property_
* 🔶 _Capability check_. Cannot have that `T($y) owns r($B)[]` (in this case, must delete entire lists instead!)
* ➖_Type check_ Must have `T(y) = r(B)`.

#### **Case ATT_LIST_OF_DEL**
* 🔶 `$y of $x` deletes any `r(y) = [l_1, l_2, ... ] :! [A](r(x) : A[].O)` where we must have `A(y) = [A]` for some `A : ATT`, and
  replaces any `l_i :! r(B)(r(x) : r(B).O)` by `l_i :! B'()`
* ➖`$B[] of $x` deletes any `l = [l_1, l_2, ... ] :! r(B)(r(x) : r(B).O)` and replaces any `l_i :! r(B)(r(x) : r(B).O)` by `l_i :! B'()`

### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Update behavior

### Basics of updating

A `update` clause comprises collection of _update statements_.

* _Input crow_: The clause can take as input a stream `{ r }` of concept rows `r`, in which case
    * the clause is **executed** for each row `r` in the stream individually

* _Updating input rows_: Update clauses do not update bindings of their input crow `r`

#### (Theory) Execution

* _Execution_: An `update` clause is executed by executing its statements individually in any order.
    * STICKY: this might be non-deterministic if the same thing is updated multiple times, solution outlined here: throw error if that's the case!

#### (Feature) Optionality

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
    * `try` blocks in `delete` clauses cannot be nested
    * `try` blocks variables are **determined** if they are supplied by `r`
    * If any variable is not determined, the `try` block statements are **skipped**.
    * If all variables are determined, the `try` block statements are **runnable**.

### Update statements

#### **Case LINKS_UP**
* 🔶 `$x links ($I: $y);` updates `r(x) :! A(b:J)` to `r(x) :! A(r(x) : r(I))`

_System property_:

1. 🔶 Require there to be exactly one present roleplayer for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction. (STICKY: discuss!)

#### **Case LINKS_LIST_UP**
* 🔶 `$x links ($I[]: <T_LIST>)` updates `r(x) :! A(j : [r(I)])` to `r(x) :! A(l : [r(I)])` for `<T_LIST>` evaluating to `l = [l_0, l_1, ...]`

_System property_:

1. 🔶 Require there to be a present roleplayer list for update to succeed (can have at most one).
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_UP**
* 🔶 `$x has $B $y;` updates `b :! r(B)(x:r(B).O)` to `r(y) :! r(B)(x:r(B).O)`

_System property_:

1. 🔶 Require there to be exactly one present attribute for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_LIST_UP**
* 🔶 `$x has $A[] <T_LIST>` updates `j :! [r(A)](r(x) : r(A).O)` to `l :! [r(A)](r(x) : r(A).O)` for `<T_LIST>` evaluating to `l = [l_0, l_1, ...]`

_System property_:

1. 🔶 Require there to be a present attribute list for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete stage.

## Put behavior

* 🔶 `put <PUT>` is equivalent to
  ```
  if (match <PUT>; check;) then (match <PUT>;) else (insert <PUT>)
  ```
  In particular, `<PUT>` needs to be an `insert` compatible set of statements. 

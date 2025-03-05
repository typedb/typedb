---
status: complete
---

# Reading data

Specification of how to read data from database. On this page:

_**Query PROCESSING**_

1. **Variable categorization**:
   2. We distinguish by input/output/internal variables
   3. We distinguish by "varcategory" (whether type, instance, list, or value)
2. **Plannability check**:
   1. Variables must be "produceable", 
   2. Subqueries must be "acyclic"
3. **Let-binding check**: let-bindings must be unique
4. **Type check**: non-type variables must be typable

_**Query EVALUATION**_

1. **Match stage** evaluation
2. **Other stage** evaluation
4. **Function and pipeline** evaluation

_**Answer FORMATTING**_

1. **Fetch** JSON

# PROCESSING

## Variable categorization

### IO Categorization algorithm

#### For stages with patterns

Given a stage with pattern `P` and set `IN` of input variables, we categorize variables `$x` for that stage as follows

* **Input variables** `$x : IN`
* **Produced variable** `$x : PROD`, if `$x` is not an input and appears anywhere in `P` which is not in a `not`-scope
* **Output variables** `$x : OUT = IN + PROD`
* **Internal variable** `$x : INTL` otherwise

#### For stages without patterns (modifiers)

Straight-forward

### Varcategory categorization algorithm

There are four varcategories: `Varcategory = { Type_, Inst_, Val_, List_ }`. Given a set of variables `S`, a **varcategory categorization** of `S` is a function `_cat : VARS -> Varcategory`. 

We define an algorithm to derive the varcategory categorization of variables in the stages of a function or pipeline as follows.

#### For match stage with inputs

Given a pattern `P` and set `IN` of **input variables** with a varcategory categorization `_cat : IN -> Varcategory`, extend this to a categorization `_cat : IN + PROD -> Varcategory` as follows:

* set `_cat($x) = Type_` exactly if `_cat($x) = Type_` or `$x` is used in _any_ **type binding position** in `P`
* set `_cat($x) = Val_` exactly if `_cat($x) = Val_` or `$x` is used in _any_ **value binding position**: 
* 🔶 set `_cat($x) = List_` exactly if `_cat($x) = List_` or `$x` is used in _any_ **list binding position**
* set `_cat($x) = Inst_` exactly if `_cat($x) = Inst_` or `$x` is used in _any_ **concept binding position** and _no_ value binding position and _no_ list binding position

If this does not define a function `p` (either because the assignment is defined multiple times or not at all), then varcategory categorization **fails**. 

Note that variable categorization happens **globally** disregarding scopes (e.g. of branches in a disjunction).

#### For nested negations

Run the above for the negated pattern, with the varcategory categorization of the **outer pattern as input**.

#### For other stages

Straight-forward.

#### For functions and pipelines

Run the above algorithm **stage by stage** with the function's arguments as initial inputs (***input types determine varcategories***). Propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

_Reject_ if varcategorization fails.

## Plannability

### Variable produceability check

We define the **variable produceability check** algorithm that checks produceability of variables in stages of a function or pipeline as follows.

#### For match stage with inputs

Assume a pattern `P` with input variables. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`. For each `$x : PROD` run the following:

1. For each non-negated statement `S` in `P`:
   * If `S` does not contain `$x`, say `S` **ignores** `$x`
   * If `S` contains `$x` in a binding position, say `S` **produces** `$x`
   * If `S` contains `$x` in a non-binding position, say `S` **requires** `$x`
2. For each `P_i = S_1; S_2; ...; S_n;`
   * If at least one `S_j` requires `$x` but no `S_j` produces `$x` **fail**
   * otherwise **succeed**.

_Remark_. (1) Function arguments of **inlineable functions** could be considered to "produces" their argument variables if these arguments are produced in the function body, see [planner spec](planner.md). (2) If each `P_i` contains `$x` you can think of `$x` as "**non-optional**", and otherwise as "**optional**". But these distinctions aren't needed to specify the desired behavior.

#### For nested negations

Run the above for the negated pattern, with the input and output of the **outer pattern as input**.

#### For other stages

Not applicable (only `match` stage produces variable in read pipeline). 

_Note_. For **write pipelines**, `insert` may also produce variables, but the produceability check is trivial: no statement in an insert requires variables; inserts only contain producing statements.

#### For functions and pipelines

Run the above algorithm **stage by stage** with the function's arguments as initial inputs. Propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

_Reject_ if produceability check fails.

### Acyclic subquery dependency check

A **query computation** is a program represented by evaluating a single **compute graph**, even if this requires expanding the graph indefinitely, see [executor spec](executor.md). A **subquery** is a computation started from a parent query that must be evaluated to **termination** before continuing with the parent query. Subqueries arise in two ways:

* `not` (need to exhaustively search to proof non-existence)
* `reduce` (need to compute all results to compute the aggregate)
* `sort` (same)

Since we allow called functions **recursively**, we must check that subquerying is acyclic: i.e., no subquery can depend on itself. Otherwise, reject the query.

## Let-binding uniqueness

We define the **let-binding uniqueness check** algorithm, that checks let-binding uniqueness for variables in stages of a function or pipeline as follows.

#### Match stage with inputs

Assume a pattern `P` with input variables `IN`. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`. 

* For each `$x : IN` ensure `P` contains no non-negated `let` statements `$x`, otherwise **fail**
* For each `$x : PROD` ensure each `P_i` contains at most one non-negated `let` statement binding `$x`, otherwise **fail**

#### For nested negations

Run the above for the negated pattern, with input and output of the **outer pattern as input**.

#### For other stages

Not applicable (`let` binding only appear in `match`).

#### For Functions and pipelines

Run the above algorithm with the function's arguments as inputs (***input types determine varcategories***). If the function as multiple `match` stages then propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

_Reject_ if check fails.

## Type check

A **type assignment** of a set of variables `VAR` is a function `_type` that assigns to each `$x : VAR` a type in our type system. The exact assignment depends on the varcategory:
* `_cat($x) = Type_` then `_type($x)` is a list of labelled types `[A_1, A_2, ...]`, `A_i : LABEL` (see [LABEL in type system](type_system.md)), Note: the list **may be empty**.
* `_cat($x) = List_` then `_type($x)` is a list types `(A_1 + A_2 + ...)[] : LIST`, `A_i : ALG` (see [LIST in type system](type_system.md)), sum can never be empty
* `_cat($x) = Value_` then `_type($x)` is a sum of values types `A_1 + A_2 + ... : ALG`, `A_i : VAL` (see [VAL in type system](type_system.md)), sum can never be empty
* `_cat($x) = Inst_` then `_type($x)` is a sum of ERA types `A_1 + A_2 + ... : ALG`, `A_i : ERA` (see [ERA in type system](type_system.md)), sum can never be empty

We define a **type-checking** algorithm that derives a type assignment for non-type variables in stages of a function or pipeline as follows.

#### Match stage with inputs

Assume a pattern `P` with input variables `IN` and a typing `_type` of those input variables. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`. Since variables have _consistent varcategories_ across branches of the DNF, we define typing by

* First construct `_type_i($x)` for each `P_i` separately as detailed below
* Then set `_type($x)` to be the sum `_type_1($x) + _type_2($x) + ...`, where the `+` means:
  * for varcat `Type_`, summing `+` is list concatenation
  * for varcat `List_`, summing `+` is `A[] + B[] = (A + B)[]`
  * for varcat `Val_`, summing `+` is just regular sum types
  * for varcat `Inst_`, summing `+` is just regular sum types

For each `P_i = S_1; S_2; ...` determine `_type_i($x)` as follows
* First:
  * If `$x` is input set `_type_i($x) = _type($x)` from input
  * Otherwise, set:
    * for varcat `Type_`, start with all of `[ERA]` 
    * for varcat `List_`, start with all of `(LABEL)[]`
    * for varcat `Val_`, start with all of `VAL`
    * for varcat `Inst_`, start with all of `ERA`
* Inductively construct a "next version" of `_type_i($x)` for the subpattern `S_1; S_2; ...; S_j;` from the "previous version" `_type_i($x)` for the subpattern `S_i; S_2; ...; S_{j-1}` as follows:
  * If `S_j` is a statement containing variable `$x, $y, ...` keep only possible type combinations in `_type_i` for those variable. _Note_: it might make sense for planning and execution to keep track of the possible **"type tuples"** `(A, B, ...)` for vars `$x, $y, ...` in the statement
  * If `S_j` is a nested negation, proceed as below, with previous `_type_i` as input, constructing the next version of `_type_i`

_Note_: We need **not reorder** `S_1; S_2; ...` so that `S_j`'s requiring `$x` came after `S_i`'s that produce `$x`. Keep the order as is can be used for improved **error messaging**.

#### For nested negations

Run the above for the negated pattern, with input and typing (as construct at the time in point when the negation is encountered) and output of the **outer pattern as input**.

#### For other stages with inputs

Straight-forward. 

_Note_: similar applies to patterns in write stages.

#### For functions and pipelines

Run the above algorithm **stage by stage** with the function's arguments and types as initial inputs (**for input types subtypes should be added** as well to typing `_type`). Propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

_Reject_ if non-type variables have no possible types.

# EVALUATION

We define the **evaluation algorithm**, which produces mult-sets (i.e. sets _with duplicates_) of **concept rows**, in three cases:

1. **patterns** with inputs:
   1. an inductive **algorithm** for their evaluation, using
   3. evaluation of **nested negations**
   2. satisfiability of **statement**
3. **modifiers** with in inputs
4. **functions** with inputs

## Match stage

Given a pattern `P` with input variables `IN` and a concept row multiset `ROWS` whose rows contain all variables of `IN` but no variables of `PROD` or `INTL`, we define the evaluation of `P` to be the multiset, obtained by

* for each row `r` in `ROWS`, obtain the (**deduplicated**!) answer _set_ `EV(P @ r)` by the procedure described in the next sections
* take the multiset union of `EV(P @ r)` for all `r` in `ROWS`

***Notation***. `EXPR @ s` means something like _"substitute all applicable mappings of `s` in `EXPR`"_.

### Algorithm

Let `P = P_1 + P_2 + ... P_m` be the DNF of `P`. We evaluate `P` branch by branch (see [planner spec](planner.md) for how to make this **more efficient**). 

* `EV(P @ r)` is defined and the deduplicated union of all `EV(P_i @ r)`

Assume `P_i = S_1; S_2; ...; S_n`.

* First, note that each `S_j` is either a **statement** or a **nested negation**

    _Note on **try**_: We expand `try { Q }` to `{ Q } or { not { Q }; }`.
* By the variable produceability check we can pick an order in which `S_j` requires only variable that have been produced by some `S_k`, `k < j`
* Inductively, define `EV((S_1; S_2; ...; S_j) @ r)` to be the set of rows obtained by:
  * taking a row `s` from `EV((S_1; S_2; ...; S_{j-1}) @ r)`
  * if `S_j` is a statement, return extensions `r` of `s` that add variable so that `S_j` is **satisfied**, see next sections (there may be zero such extension)
  * if `S_j` is a nested negation, return `s` iff `S_j @ s` evaluates to **true**, see next sections.

### Nested negations

Given `S = not { Q }` with input row `s`, then `S @ s` is true iff `EV(Q @ s)` is empty.

### Statements

For row `r` extending `s` in algorithmic step above, we define what this means for `r` to **satisfy** statement `S_j` (note this covers both the case of "producing")

#### Type variable statements

* `kind $A` is satisfied if `$A @ r: KIND` (can have `KIND = ENTITY | RELATION | ATTRIBUTE`)
* `$A sub $B` is satisfied if `_kind($A @ r) = _kind($B @ r)` and `$A @ s < $B @ s` (***here***: can have `KIND = ENTITY | RELATION | ATTRIBUTE | ROL`)
  * `$A sub! $B` is satisfied if `$A @ r : KIND`, `$B @ r : KIND`, `$A @ r <! $B @ r`
* `$A value $V` is satisfied if `$A @ r <= B` and `_val : B -> $V @ r`
* `$A relates $I` is satisfied either if `$A @ r : REL($I @ r)` ***or*** `#($A @ r : REL($I @ r))` (***abstract branch***), [for `#(...)` notation see type system](type_system.md)
  * `$A relates! $I` is satisfied if `$A relates $I` ***and*** `B : REL($I @ r)` or `#(B : REL($I @ r))` necessarily implies `B <= $A @ r`
  * `$A relates $I as $J` is satisfied if `$A relates $I` ***and*** there exists `B relates $J` such that `A$ @ r <= B` and `$I @ r <= $J @ r`
  * `$A relates! $I as $J` is satisfied if `$A relates $I as $J` ***and*** `$A relates! $I`
* 🔶 `$A relates $I[]` is satisfied either `$A @ r : REL($I[] @ r])` ***or*** `#($A @ r : REL($I[] @ r))` (***abstract branch***)
  * 🔶 `$A relates! $I[]` is satisfied if `$A relates $I[]` ***and*** `B : REL($I[] @ r)` or `#(B : REL($I[] @ r))` necessarily implies `B <= $A @ r`
  * 🔶 `$A relates $I[] as $J[]` is satisfied if `$A relates $I[]` ***and*** there exists `B relates $J[]` such that `A$ @ r <= B` and `$I @ r <= $J @ r`
  * 🔶 `$A relates! $I[] as $J[]` is satisfied if `$A relates $I[] as $J[]` ***and*** `$A relates! $I[]`
* `$A plays $I` is satisfied if `$A @ r <= B`, `B : _kind($A @ r)`, and `B <! $I @ r` **or** `#(B <! $I @ r)` (***abstract branch***)
  * `$A plays! $I` is satisfied if `$A plays $I` and `B plays $I` necessarily implies `B <= $A @ r`
* `$A owns $B` is satisfied if `$A @ r <= C`, `C : _kind($A @ r)`, and either `C <! $B.O @ r)` ***or*** `#(C <! $B.O @ r)`  (***abstract branch***)
  * `$A owns! $B` is satisfied if `$A owns $B` and `C owns $B @ r` implies `C <= $A @ r`
* 🔶 `$A owns $B[]` is satisfied if `$A @ r <= C`, `C : _kind($A @ r)`, and `C <! $B[].O @ r` ***or*** `#(C <! $B[].O @ r)`  (***abstract branch***)
  * 🔶 `$A owns! $B[]` is satisfied if `$A owns $B[]` and `C owns $B[] @ r` implies `C <= $A @ r`
* `$A label <LABEL>` is satisfied if `$A @ r` has primary label or alias `<LABEL>`

#### Abstractness in type variable statements

* `kind $B @abstract` is satisfied if `kind $B @ r @abstract` is defined in the schema (note, abstractness for type identity is not directly recorded in [type system](type_system.md))
* `<statement> @abstract` is satisfied if `<statement>` holds as defined above but the **abstract branch of the satisfaction condition** is taken. This applies to:
  * `$A relates $I @abstract`
  * `$A relates! $I @abstract`
  * 🔶 `$A relates $I[] @abstract`
  * 🔶 `$A relates! $I[] @abstract`
  * `$A plays $B:$I @abstract`
  * `$A plays! $B:$I @abstract`
  * `$A owns $B @abstract`
  * `$A owns! $B @abstract`
  * 🔶 `$A owns $B[] @abstract`
  * 🔶 `$A owns! $B[] @abstract`

_Remark_. To indicate we mean the opposite (non-abstract) branch of the satisfaction condition, could consider `@nonabstract` or `@concrete` (just for `match` pattern statements).

#### Constraints in type variable statements

* ***cannot match*** `@card(n..m)` (STICKY: there's just not much point to do so ... rather have normalized schema dump. discuss `@card($n..$m)`??)

* ***cannot match*** `@values/@regex/@range` (STICKY: there's just not much point to do so ... rather have normalized schema dump)

For `@MODE = @unique | @key | @subkey(<LABEL>) | @distinct`:

* `$A owns $B @MODE` is satisfied if, for some `C`, `$A @ r sub C` and `C owns $B @ r @MODE` is in defined in schema
* `$A owns! $B @MODE` is satisfied if `A @ r owns $B @ r @MODE` is in defined in schema
* 🔶 `$A owns $B[] @MODE` is satisfied if, for some `C`, `$A @ r sub C` and `C owns $B[] @ r @MODE` is in defined in schema
* 🔶 `$A owns! $B[] @MODE` is satisfied if `A @ r owns $B[] @ r @MODE` is in defined in schema
* `$A relates $I @MODE` is satisfied if, for some `C`, `$A @ r sub C` and `C relates $I @ r @MODE` is in defined in schema
* `$A relates! $I @MODE` is satisfied if `A @ r relates $I @ r @MODE` is in defined in schema
* 🔶 `$A relates $I[] @MODE` is satisfied if, for some `C`, `$A @ r sub C` and `C relates $I[] @ r @MODE` is in defined in schema
* 🔶 `$A relates! $I @MODE` is satisfied if `A @ r relates $I @ r @MODE` is in defined in schema

#### Non-type (data) variable statements

* `$x isa $T` is satisfied if `$x @ r : $T @ r` for `$T @ r : ERA`
    * `$x isa! $T` is satisfied if `$x @ r :! $T @ r` for `$T @ r : ERA`
    * `$T` is equivalent to `$_ isa $T`
* `$x isa $T ($I: $y, $J: $z, ...)` is satisfied if `$x @ r : $T ($y : $I, $z : $J, ...) @ r` for `$T @ r : REL`
    * `$x isa! $T ($I: $y, $J: $z, ...)` is satisfied if `$x @ r :! $T ($y : $I, $z : $J, ...) @ r` for `$T @ r : REL`
    * `$T ($I: $y, $J: $z, ...)` is equivalent to `$_ isa $T ($I: $y, $J: $z, ...)`
* `$x isa $T EXPR` is satisfied if `$x @ r : $T @ r`, `_val($x @ r) = EXPR @ r` for `$T @ r : ATT` (**reject** if `EXPR` is a single variable of varcat `Inst_`)
    * `$x isa! $T EXPR` is satisfied if `$x @ r :! $T @ r`, `_val($x @ r) = EXPR @ r` for `$T @ r : ATT` (**reject** if `EXPR` is a single variable of varcat `Inst_`)
    * `$T EXPR` is equivalent to `$_ isa $T EXPR` (**reject** if `EXPR` is a single variable of varcat `Inst_`)
* `$x links ($I: $y, $J: $z, ...)` is satisfied if `$x @ r : A($y : $I, $z : $J, ...) @ r` for some `A : REL($I @ r, $J @ r, ...)`.
    * `$x links! ($I: $y)` is satisfied if `$x @ r :! A($y : $I, $z : $J, ...) @ r` for some `A : REL($I @ r, $J @ r, ...)`.
    * `$x links ($y)` is equivalent to `$x links ($_: $y)` 
* 🔶 `$x links ($I[]: $y, ...)` is satisfied if `$x @ r : A($y : $I[]) @ r` for some `A : REL($I[], ...) @ r`.
    * 🔶 `$x links ($I[]: <LIST_EXPR>, ...)` is equivalent to `$x links ($I[]: $_y, ...); $_y == <LIST_EXPR>;` for anonymous `$_y`
    * 🔶 `$x links! ($I[]: $y, ...)` is satisfied if `$x @ r :! A($y : $I[] , ...) @ r` for some `A : REL($I[] @ r)`.
* `$x has $B $y` is satisfied if `$y @ r : $B ($x : $B.O) @ r` for `$B @ r : ATT`.
    * `$x has! $B $y` is satisfied if `$y @ r :! $B @ r($x @ r:$B.O @ r)` for some `$B @ r : ATT`.
    * `$x has(!) $B EXPR` is satisfied if `$x @ r :(!) $T @ r`, `_val($x @ r) = EXPR @ r` for `$T @ r : ATT` (**reject** if `EXPR` is a single variable of varcat `Inst_`)
    * `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`

    _Remark_. Note that `$x has $B $y` will match the individual list elements of list attributes.

* 🔶 `$x has $B[] $y` is satisfied if `$y @ r : $B[]($x : $B[].O) @ r` for some `$B @ r : ATT`.
    * 🔶 `$x has! $B[] $y` is satisfied  if `$y @ r :! $B[]($x : $B[].O) @ r`, for some `$B @ r : ATT`
    * 🔶 `$x has(!) $B[] EXPR` is satisfied  if `a :(!) $B[]($x : $B[].O) @ r`, `a = EXPR @ r` for some `$B @ r : ATT` (**reject** if `EXPR` is a single variable of varcat `Inst_`)

* `$x is $y` is satisfied if `_cat($x) = _cat($y)` and `$x @ r = $y @ r` (**reject** if `_cat($x) = Val_`)

#### Expression, values, functions

**_Note_**: `EXPR @ r` is evaluated using usual expression semantics, and:

* we **cast** any attribute `a` to its value `_val(a)` if it appears in a value position in an expression;
* this casting also applies in **function arguments** which have value types (function are after closely related to expressions, e.g. `$x + $y = add($x, $y)`)

In the following expression evaluation is largely left implicit:

* `EXPR_1 == EXPR_2` is satisfied if `EXPR_1 @ r = EXPR_2 @ r`
* `EXPR_1 != EXPR_2` is satisfied if not `EXPR_1 @ r = EXPR_2 @ r`
* `EXPR_1 COMP EXPR_2`, where `COMP` is a comparison, is satisfied based on the usual comparison semantics
* `EXPR isnt;` is satisfied if  `EXPR @ r == ()`, **requires** all its vars
* `let $x = EXPR;` is satisfied if  `$x` in `EXPR @ r`
* `let $x = f($y);` is satisfied if  `$x` in `EV(f($y) @ r)` (with evaluation for functions/pipelines as defined above)
* `let $x? = f($y);` equivalent to `{ let $x in f($y); } or { f($y) isnt; };`.
* `let DESTRUCT = STRUCT` with grammar
    ```
    STRUCT   ::=  VAR 
                  | { LABEL: (VAL|VAR|STRUCT), ... }
                  | HINT { : (VAL|VAR|STRUCT), ... }
    DESTRUCT ::=  { LABEL: (VAR|VAR?|DESTRUCT), ... }        
    ```
  where `HINT` is a schema-defined **struct name**, and `COMP` is a schema-defined **component name**:

  The statement is satisfied if `DESTRUCT @ r = STRUCT @ r` **and** only variables marked with `?` in `DESTRUCT` may be assigned `()`.
* 🔶`let $x in $l` is satisfied if `$x @ r _in $l @ r`
    * 🔶 `let $x in LIST_EXPR` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar")
* 🔶 `$l contains $x` is satisfied if `r(x) in r(l)`
    * 🔶 `LIST_EXPR contains $x` is equivalent to `let $_l = <LIST_EXPR>; $_l contains $x;`

## Other stages

Other stages are mainly straight-forward. (Feel free to add notes about their intricacies here):

* `select`
* `limit`
* `offset`
* `reduce`
* `distinct`

## Functions and pipelines

Evaluate functions stage by stage. For recursive functions, augment recursive calls with "depth" (instead of `f` calling `f`, let `f_n` call `f_{n-1}`) which breaks the recursion. Then evaluation becomes easy to describe: to evaluate a call to `f` up to recursive depth, say, `1000`, replace the call with a call to `f_1000`.

Recursion can be more elegantly described in terms of "cycle-breaking" expansions of computation graph, see [executor spec](executor.md).

# FORMATTING

## Fetch JSON

A `fetch JSON` clause is of the form

  ```
  fetch { 
   KV-STATEMENT;
   ...
   KV-STATEMENT;
  }
  ```
with the following KV statement cases:

* `"key": $x`
* `"key": EXPR`
* `"key": $x.A` where `A : ATT`
* `"key": [ $x.A ]` where `A : ATT`
* `"key": $x.A[]` where `A : ATT`
* `"key": { $x.* }` where `A : ATT` and we format individual attributes as:
    * `"att_label" : [ <atts> ]`
    * `"att_label[]" : <att-list>`.
* `"key": fun(...)` where `fun` is **scalar** value
* `"key": [ fun(...) ]` where `fun` is **scalar stream** value
* Fetch list of JSON sub-documents:
    ```
    "key": [ 
      <READ_PIPELINE>
      fetch { <FETCH> }
    ]
    ```
* Fetch single-value:
    ```
    "key": ( 
      <READ_PIPELINE>
      return <SINGLE> <VAR>; 
    )
    ```
* Fetch single-value:
    ```
    "key": [ 
      <READ_PIPELINE>
      return { <VAR> }; 
    ]
    ```
* Fetch stream as list:
    ```
    "key": [ 
      <READ_PIPELINE>
      return <AGG>, ... , <AGG>; 
    ]
    ```

    This is short hand for:
    ```
    "key": [ 
      <READ_PIPELINE>
      reduce $_1? = <AGG>, ... , $_n? = <AGG>; 
      return first $_1, ..., $_n; 
    ]
    ``` 
* Specify JSON sub-document:
    ```
    "key" : { 
      <FETCH-KV-STATEMENT>;
      ...
      <FETCH-KV-STATEMENT>;
    }
    ```

## ? Fetch GRAPH

TBD
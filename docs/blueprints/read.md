# Reading data

Specification of how to read data from database. On this page:

_**Query PROCESSING**_

1. **Variable categorization**:
   2. We distinguish by input/output/checked variables
   3. We distinguish by "vartype" (whether type, instance, list, or value)
2. **Plannability check**:
   1. Variables must be "produceable", 
   2. Subqueries must be "acyclic"
3. **Let-binding check**: let-bindings must be unique
4. **Type check**: non-type variables must be typable

_**Query EVALUATION**_

1. **Pattern** evaluation
2. **Modifier** evaluation
3. **Nested negation** evaluation
4. **Function** evaluation

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
* **Checked variable** `$x : CHECK` otherwise

#### For stages without patterns (modifiers)

Straight-forward

### Vartype categorization algorithm

There are four vartypes: `Vartype = { type_, inst_, val_, list_ }`. Given a set of variables `S`, a **vartype categorization** of `S` is a function `_cat : VARS -> Vartype`. 

#### For match stage with inputs

Given a pattern `P` and set `IN` of **input variables** with a vartype categorization `_cat : IN -> Vartype`, extend this to a categorization `_cat : IN + PROD -> Vartype` as follows:

* set `_cat($x) = type_` exactly if `_cat($x) = type_` or `$x` is used in _any_ **type binding position** in `P`
* set `_cat($x) = val_` exactly if `_cat($x) = val_` or `$x` is used in _any_ **value binding position**: 
* 🔶 set `_cat($x) = list_` exactly if `_cat($x) = list_` or `$x` is used in _any_ **list binding position**
* set `_cat($x) = inst_` exactly if `_cat($x) = inst_` or `$x` is used in _any_ **concept binding position** and _no_ value binding position and _no_ list binding position

If this does not define a function `p` (either because the assignment is defined multiple times or not at all), **reject** the pattern based on failed vartype categorization. Note that variable categorization happens **globally** disregarding scopes (e.g. of branches in a disjunction).

#### For nested negations

Run the above for the negated pattern, with the vartype categorization of the **outer pattern as input**.

#### For other stages

Straight-forward.

#### For functions and pipelines

Run the above algorithm with the function's arguments as inputs (***input types determine vartypes***). If the function as multiple stages then propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

## Plannability

### Variable produceability check

We define the **variable produceability check** algorithm.

#### For match stage with inputs

Assume a pattern `P` with input variables. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`. For each `$x : PROD` run the following:

1. For each non-negated statement `S` in `P`:
   * If `S` does not contain `$x`, say `S` **ignores** `$x`
   * If `S` contains `$x` in a binding position, say `S` **produces** `$x`
   * If `S` contains `$x` in a non-binding position, say `S` **requires** `$x`
2. For each `P_i = S_1; S_2; ...; S_n;`
   * If at least one `S_j` requires `$x` but no `S_j` produces `$x` *reject*
   * otherwise *accept*.

_Remark_. (1) Function arguments of **inlineable functions** could be considered to "produces" their argument variables if these arguments are produced in the function body, see [planner spec](planner.md). (2) If each `P_i` contains `$x` you can think of `$x` as "**non-optional**", and otherwise as "**optional**". But these distinctions aren't needed to specify the desired behavior.

#### For nested negations

Run the above for the negated pattern, with the input and output of the **outer pattern as input**.

#### For other stages

Straight-forward.

#### For functions and pipelines

For functions, run the above algorithm with the function's arguments as inputs (***input types determine vartypes***). If the function as multiple stages then propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

### Acyclic subquery dependency check

A **query computation** is a program represented by evaluating a single **compute graph**, even if this requires expanding the graph indefinitely, see [executor spec](executor.md). A **subquery** is a computation started from a parent query that must be evaluated to **termination** before continuing with the parent query. Subqueries arise in two ways:

* `not` (need to exhaustively search to proof non-existence)
* `reduce` (need to compute all results to compute the aggregate)

Since we allow called functions **recursively**, we must check that subquerying is acyclic: i.e., no subquery can depend on itself. Otherwise, reject the query.

## Let-binding uniqueness

We define the **let-binding uniqueness check** algorithm.

#### Match stage with inputs

Assume a pattern `P` with input variables `IN`. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`. 

* For each `$x : IN` ensure `P` contains no non-negated `let` statements `$x`, otherwise _reject_
* For each `$x : PROD` ensure each `P_i` contains at most one non-negated `let` statement binding `$x`, otherwise _reject_

#### For nested negations

Run the above for the negated pattern, with input and output of the **outer pattern as input**.

#### For other stages

Straight-forward.

#### For Functions and pipelines

Run the above algorithm with the function's arguments as inputs (***input types determine vartypes***). If the function as multiple `match` stages then propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

## Type check

We define a **type-checking** algorithm.

#### Match stage with inputs

Assume a pattern `P` with input variables `IN`. Let `P_0 + P_1 + ... + P_k` be the **DNF** of `P`.

* For each `$x : IN` ensure `P` contains no non-negated `let` statements `$x`, otherwise _reject_
* For each `$x : PROD` ensure each `P_i` contains at most one non-negated `let` statement binding `$x`, otherwise _reject_

#### For nested negations

Run the above for the negated pattern, with input and output of the **outer pattern as input**.

#### For other stages

Straight-forward.

#### For Functions and pipelines

Run the above algorithm with the function's arguments as inputs (***input types determine vartypes***). If the function as multiple `match` stages then propagate outputs of earlier stages as inputs to later stages.

A read pipeline is just a function with no inputs.

# EVALUATION

We define the **evaluation algorithm**, which produces mult-sets (i.e. sets _with duplicates_) of **concept rows**, in three cases:

1. **patterns** with inputs:
   1. an inductive **algorithm** for their evaluation, using
   3. evaluation of **nested negations**
   2. satisfiability of **statement**
3. **modifiers** with in inputs
4. **functions** with inputs

## Patterns

Given a pattern `P` with input variables `IN` and a concept row multiset `ROWS` whose rows contain all variables of `IN` but no variables of `PROD` or `CHECK`, we define the evaluation of `P` to be the multiset, obtained by

* for each row `r` in `ROWS`, obtain the (**deduplicated**!) answer _set_ `EV(P[IN/r])` by the procedure described in the next sections
* take the multiset union of `EV(P[IN/r])` for all `r` in `ROWS`

### Algorithm

Let `P = P_1 + P_2 + ... P_m` be the DNF of `P`. We evaluate `P` branch by branch (see [planner spec](planner.md) for how to make this **more efficient**). 

* `EV(P[r])` is defined and the deduplicated union of all `EV(P_i[r])`

Assume `P_i = S_1; S_2; ...; S_n`.

* First, note that each `S_j` is either a **statement** or a **nested negation**

    _Note on **try**_: We expand `try { Q }` to `{ Q } or { not { Q }; }`.
* By the variable produceability check we can pick an order in which `S_j` requires only variable that have been produced by some `S_k`, `k < j`
* Inductively, define `EV(S_1; S_2; ...; S_j[r])` to be the set of rows obtained by:
  * taking a row `s` from `EV(S_1; S_2; ...; S_{j-1}[r])`
  * if `S_j` is a statement, return extensions of `s` that add variable so that `S_j` is **satisfied**, see next sections (there may be zero such extension)
  * if `S_j` is a nested negation, return `s` iff `S_j[s]` evaluates to **true**, see next sections.

### Nested negations

Given `S = not { Q }` with input row `s`, then `S[s]` is true iff `EV(Q[s])` is empty.

### Statements

## Modifiers

## Recursive queries



## Pattern semantics

### Typing satisfaction

* For tvars `$X` in `PATT`, `T($X)` is a type kind (`entity`, `attribute`, `relation`, `trait`, `value`)
* For vvars `$x` in `PATT`, `T($x)` is a value type (primitive or struct)
* For lvars `$x` in `PATT`, `T($x)` is a list type `A[]` for ***minimal*** `A` a type such that `A` is the minimal upper bounds of the types of the list elements `<EL>` in the list `r($x) = [<EL>, <EL>, ...]` (note: our type system does have minimal upper bounds thanks to sums)
* For ivars `$x` in `PATT`, `T($x)` is a schema type `A` such that `r(x) :! A` isa **direct typing**

### Pattern satisfaction

#### Block-level bindings

Given a (valid and unambiguous) pattern `PATT` in DNF, we define for subpatterns

* `not { SUBPATT };`
* `try { SUBPATT };`

a variable `$x` in `SUBPATT` is **block-level** if its bound in statements that appears unnested in a branch of `SUBPATT` but not a parent block.

#### Recursive definition

We define satisfication of a pattern `PATT` in DNF by an input crow `r`. The definition recursively destructures the pattern into subpatterns `P` (to begin, set `P = PATT`).

* If `P = STMT; SUBPATT` then `r` satisfies `P` if:
    * The statement **<STMT>** is satisfied by `r` as outlined in the _next sections_.
    * `r` satisfies `SUBPATT`;

* If `P = { SUBPATT_1 } or { SUBPATT_2 } or ... ;` then `r` satisfies `P` if:
    * `r` satisfies `SUBPATT_i` for any `i`

* If `P = not { SUBPATT };` then `r` satisfies `P` if:
    * `r` does _not contain any_ block-level bound variables of the block _except_ input vars
    * `r` cannot be completed with entries for block-level bound variable to satisfy `SUBPATT`

* If `P = try { SUBPATT };` then `r` satisfies `P` if:
    * `r` _contains all_ block-level bound variables of the block
    * and:
        * either: `r` satisfies `SUBPATT`
        * or:
            * all block-level bound variables are assigned `()` (i.e. empty concept) by `r` _except_ input vars
            * after obtain `r'` from `r` by removing block-level bound, non-input variables, `r'` cannot be completed with entries for block-level bound variable to satisfy `SUBPATT`

### `Let` declarations in patterns

_System property_

The keyword `let` has two special properties:

* 🔶 `let` assignments must be acyclic
* 🔶 No variable can be `let` assigned twice

## Satisfaction semantics of...

We discuss the satisfaction semantics of various classes of statements.

_Math. notation (Replacing **var**s with concepts)_. When discussing pattern semantics, we always consider **fully variablized** statements (e.g. `$x isa $X`, `$X sub $Y`). This also determines satisfaction of **partially assigned** versions of these statements (e.g. `$x isa A`, `$X sub A`, `A sub $Y`, or `x isa $A`).

## ... Function statements

### **Case LET_FUN_PATT**

_Remark_ the following can be said in less space, but we chose the more principled longer route, via "single returns".

* 🔶 `let _, ..., _, $x, _, ..., _ = f($a, $b, ...)` (where `$x` is in $i$th position of the comma-separated list of length $n$, and all other positions are "blanks" `_`). This statement is satisfied if:
    * denoting *function evalution* (see "Function evaluation") with inputs from the crow `r` by `ev(f(r(a), r(b), ...)`,
    * `_ev(f(r(a), r(b), ...)` contains exactly one row `w` of length `n` (if it contains no row we reject)
    * `r(x)` is the $i$th entry of `w` (corresponding to `$x` being used in `i`$th position on the LHS)

  _Note_. This is equivalent to `$x in F_i($a, $b, ...)` where `F_i` modifies `F` with an additional selection of the `i`th variable.

* 🔶 `let $x, $y?, ..., $w = f($a, $b, ...)` is satisfied if the following pattern is satisfied:
    ```
    let $x,_,...,_ in F($a, $b, ...);               // first var (non-optional!)
    try { let _, $y, ...,_ in F($a, $b, ...); };    // second var (optional!)
    ...                                             // ...
    let _, _, ...,$w in F($a, $b, ...);             // last var
    ```

  _Note_. `?`-marked variables are retrieved with **separate** `try`-blocks.

* 🔶 `let $x, $y?, ..., $w = F($a, <EXPR_b>, ...)>` is satisfied if the following pattern is satisfied:
    ```
    let $_b = <EXPR_b>;
    ...
    let $x, $y?, ..., $w in F($a, $_b, ...)
    ```

_System property_

* 🔶 _Output type_ Function call must be to a **single-return** function, i.e. have output type `T, ...`.
* 🔶 _Boundedness_ All variable arguments (or variables in expression arguments) to `f` must be set in the crow `r` (i.e. should be bound somewhere else in the pattern).
* 🔶 _Acyclicity (pattern-level constraint)_ All let statements must be acyclic, e.g. we **cannot have**
    ```
    let $x = f($y); 
    let $y = f($x);
    ```

### **Case LET_IN_FUN_PATT**

* 🔶 `let $x, $y?, ..., $w in F($a, <EXPR_b>, ...)>` is satisfied if, for some choice of row `w in _ev(F(r(a),v_r(expr_b),...)` in the evaluation set of the function call such that the following pattern is satisfied:
    ```
    let $x, $y?, ..., $w = f($a, <EXPR_b>, ...)
    ```

  where `f($a, <EXPR_b>, ...)` denotes a single-return function call ***defined*** to evaluate to row `w`.

_System property_

* 🔶 _Output type_ Function call must be to a **stream-return** function, i.e. have output type `{ T, ... }`.
* 🔶 _Boundedness_ All variable arguments (or variables in expression arguments) to `F` must be set in the crow `r` (i.e. should be bound somewhere else in the pattern).
* 🔶 _Acyclicity (pattern-level constraint)_ All let statements must be acyclic, e.g. we **cannot have**
    ```
    let $x in F($y); 
    let $y in F($x);
    ```

## ... Type statements

### **Case TYPE_DEF_PATT**
* ➖ `Kind $A` (for `Kind` in `{entity, relation, attribute}`) is satisfied if `r(A) : KIND`

* ➖ `(Kind) $A sub $B` is satisfied if `r(A) : KIND`, `r(B) : KIND`, `r(A) < r(B)`
* ➖  `(Kind) $A sub! $B` is satisfied if `r(A) : KIND`, `r(B) : KIND`, `r(A) <! r(B)`

_Remark_: `sub!` is convenient, but could actually be expressed with `sub`, `not`, and `is`. Similar remarks apply to **all** other `!`-variations of TypeQL key words below.

### **Case REL_PATT**
* ➖ `$A relates $I` is satisfied
    * either if `r(A) : REL(r(I))`
    * or if `#(r(A) : REL(r(I)))`

* ➖ `$A relates $I as $J` is satisfied if
    * `r(A) : REL(r(I))` or `#(r(A) : REL(r(I)))`
    * there exists `B : REL(r(J))` or `#(B : REL(r(J)))`
    * `A <= B` and `r(I) <= r(J)`.

* 🔶 `$A relates $I[]` is satisfied if `r(A) : REL(r([I]))` and
    * either if `r(A) : REL([r(I)])`
    * or if `#(r(A) : REL([r(I)]))`

* 🔶 `$A relates $I[] as $J[]` is satisfied if
    * `r(A) : REL(r([I]))` or `#(r(A) : REL(r([I]))`
    * there exists `B : REL(r([J]))` or `#(B : REL(r([J])))`
    * `A <= B` and `r(I) <= r(J)`.

### **Case DIRECT_REL_PATT**

* 🔮 `$A relates! $I` is satisfied if
    * `r(A) : REL(r(I))` and **not** `r(A) < B : REL(r(I))`
    * or if `#(r(A) : REL(r(I)))` and **not** `#(r(A) < B : REL(r(I)))`

* 🔮 `$A relates(!) $I as! $J` is satisfied if `$A relates(!) $I` and
    * there exists `B : REL(r(J))` or `#(B : REL(r(J)))`
    * `A <! B` and `r(I) <! r(J)`

* 🔮 `$A relates! $I[]` is satisfied if
    * `r(A) : REL(r([I]))` and **not** `r(A) < r(B) : REL(r([I]))`
    * or if `#(r(A) : REL(r([I])))` and **not** `#(r(A) < r(B) : REL(r([I])))`

* 🔮 `$A relates $I[] as! $J[]` is satisfied if `$A relates(!) $I[]` and
    * there exists `B : REL(r([J]))` or `#(B : REL(r([J])))`
    * `A <! B` and `r(I) <! r(J)`

### **Case PLAYS_PATT**

* ➖ `$A plays $I` is satisfied if
    * either `r(A) <= A'` and `A' <! r(I)`
    * or if `r(A) <= A'` and `#(A' <! r(I))`

### **Case DIRECT_PLAYS_PATT**

* 🔮 `$A plays! $I` is satisfied if `r(A) <! r(I)`
    * `A <! r(I)`
    * _(to match `@abstract` for `plays!` must use annotation, see **PLAYS_ABSTRACT_PATT**)_

### **Case VALUE_PATT**

* ➖ `$A value $V` is satisfied if `r(A) <= A'` and `_val : A' -> r(V)`

### **Case OWNS_PATT**

* ➖ `$A owns $B` is satisfied if
    * either `r(A) <= A'` and `A' <! r(B).O)`
    * or `r(A) <= A'` and `#(A' <! r(B).O)`

* 🔶 `$A owns $B[]` is satisfied if `r(A) <= A' <! r(B.O)` (for `A'` **not** an trait type)
    * either `r(A) <= A'` and `A' <! r(B[].O)`
    * or `r(A) <= A'` and `#(A' <! r(B[].O))`

_Remark_. In particular, if `A owns B[]` has been declared, then `$X owns B` will match the answer `r($X) = A`.

### **Case DIRECT_OWNS_PATT**

* 🔮 `$A owns! $B` is satisfied if
    * `r(A) <! r(B.O)`
    * or `#(r(A) <! r(B.O))`

* 🔮 `$A owns! $B[]` is satisfied if `r(A) <! r(B.O)`
    * `r(A) <! r(B.O)`
    * or if `#(r(A) <! r(B.O))`

### **Cases TYP_IS_PATT and LABEL_PATT**
* ➖`$A is $B` is satisfied if `r(A) = r(B)` (this is actually covered by the later case `IS_PATT`)
* ➖`$A label <LABEL>` is satisfied if `r(A)` has **primary label or alias** `<LABEL>`

## ... Type constraint statements

### Cardinality

_To discuss: the usefulness of constraint patterns seems overall low, could think of a different way to retrieve full schema or at least annotations (this would be more useful than, say,having to find cardinalities by "trialing and erroring" through matching)._

#### **Case CARD_PATT**
* ➖ cannot match `@card(n..m)` (STICKY: there's just not much point to do so ... rather have normalized schema dump. discuss `@card($n..$m)`??)
<!-- 
* `A relates I @card(n..m)` is satisfied if `r(A) : REL(r(I))` and schema allows `|a|_I` to be any number in range `n..m`.
* `A plays B:I @card(n..m)` is satisfied if ...
* `A owns B @card(n...m)` is satisfied if ...
* `$A relates $I[] @card(n..m)` is satisfied if ...
* `$A owns $B[] @card(n...m)` is satisfied if ...
-->

### Modalities

#### **Case UNIQUE_PATT**
* 🔶 `$A owns $B @unique` is satisfied if `r(A) <= A' <! r(B.O)` (for `A'` **not** an trait type), and schema directly contains constraint `A' owns r($B) @key`.

* 🔶 `$A owns! $B @unique` is satisfied if `r(A) <! r(B.O)`, and schema directly contains constraint `r($A) owns r($B) @unique`.

#### **Case KEY_PATT**
* 🔶 `$A owns $B @key` is satisfied if `r(A) <= A' <! r(B.O)` (for `A'` **not** an trait type), and schema directly contains constraint `A' owns r($B) @key`.

* 🔶 `$A owns! $B @key` is satisfied if `r(A) <! r(B.O)`, and schema directly contains constraint `r($A) owns r($B) @key`.

#### **Case SUBKEY_PATT**
* 🔶 `$A owns $B @subkey(<LABEL>)` is satisfied if `r(A) <= A' <! r(B.O)` (for `A'` **not** an trait type), and schema directly contains constraint `A' owns r($B) @subkey(<LABEL>)`.

#### **Case TYP_ABSTRACT_PATT**
* 🔶 `(kind) $B @abstract` is satisfied if schema directly contains `(kind) r($B) @abstract`.

#### **Case REL_ABSTRACT_PATT**
* 🔶 `$B relates $I @abstract` is satisfied if:
    * `r(B) <= B'` and `#(B' : REL(I)`
    * **not** `r(B) <= B'' <= B'` such that `#(B' : REL(I)`

* 🔮 `$B relates! $I @abstract` is satisfied if `#(r(B) : REL(I)`

* 🔶 `$B relates $I[] @abstract` is satisfied if:
    * `r(B) <= B'` and `#(r(B) : REL([I])`
    * **not** `r(B) <= B'' <= B'` such that `B' : REL([I])`

* 🔮 `$B relates! $I[] @abstract` is satisfied if `#(r(B) : REL([I])`

#### **Case PLAYS_ABSTRACT_PATT**
* 🔶 `$A plays $B:$I @abstract` is satisfied if:
    * `r(A) <= A'` and `#(A' <! r(I))`
    * **not** `r(A) <= A'' <= A'` such that `A'' <! r(I)`

  where `r(B) REL(r(I))`

* 🔮 `$A plays! $B:$I @abstract` is satisfied if `#(r(A) <! r(I))`, where `r(B) REL(r(I))`

#### **Case OWNS_ABSTRACT_PATT**
* 🔶 `$A owns $B @abstract` is satisfied if
    * `r(A) <= A'` and `#(A' <! r(B).O)`
    * **not** `r(A) <= A'' <= A'` such that `A'' <!  r(B).O)`

* 🔮 `$A owns! $B @abstract` is satisfied if `#(r(A) <! r(B).O)`

* 🔶 `$A owns $B[] @abstract` is satisfied if
    * `r(A) <= A'` and `#(A' <! r(B)[].O)`
    * **not** `r(A) <= A'' <= A'` such that `A'' <!  r(B)[].O)`

* 🔮 `$A owns! $B[] @abstract` is satisfied if `#(r(A) <! r(B)[].O)`

#### **Case DISTINCT_PATT**
* 🔮 `$A owns $B[] @distinct` is satisfied if `r(A) <= A` schema directly contains constraint `A' owns r($B)[] @distinct`.
* 🔮 `$A owns! $B[] @distinct` is satisfied if schema directly contains constraint `r($A) owns r($B)[] @distinct`.
* 🔮 `$B relates $I[] @distinct` is satisfied if `r(B) : REL(r([I]))`, `B <= B'` and schema directly contains `B' relates r($I)[] @distinct`.
* 🔮 `$B relates! $I[] @distinct` is satisfied if schema directly contains `r($B) relates r($I)[] @distinct`.

### Values constraints

#### **Cases VALUE_VALUES_PATT and OWNS_VALUES_PATT**
* cannot match `@values/@regex/@range` (STICKY: there's just not much point to do so ... rather have normalized schema dump)
<!--
* `A owns B @values(v1, v2)` is satisfied if 
* `A owns B @regex(<EXPR>)` is satisfied if 
* `A owns B @range(v1..v2)` is satisfied if 
* `A value B @values(v1, v2)` is satisfied if 
* `A value B @regex(<EXPR>)` is satisfied if 
* `A value B @range(v1..v2)` is satisfied if 
-->

## ... Element statements

### **Case ISA_PATT**
* ➖ `$x isa $T` is satisfied if `r(x) : r(T)` for `r(T) : ERA`
* 🔶 `$x isa $T ($I: $y)` is equivalent to `$x isa $T; $x links ($I: $y);`
* 🔶 `$x isa $T <EXPR>` is equivalent to `$x isa $T; $x == <EXPR>;`

### **Case ANON_ISA_PATT**
* 🔶 `$T` is equivalent to `$_ isa $T`
* 🔶 `$T ($R: $y, ...)`  is equivalent to `$_ isa $T ($R: $y, ...)`
* 🔶 `$T <EXPR>` is equivalent to `$_ isa $T <EXPR>`

### **Case DIRECT_ISA_PATT**

* ➖ `$x isa! $T` is satisfied if `r(x) :! r(T)` for `r(T) : ERA`
* 🔶 `$x isa! $T ($I: $y)` is equivalent to `$x isa! $T; $x links ($I: $y);`
* 🔶 `$x isa! $T <EXPR>` is equivalent to `$x isa! $T; $x == <EXPR>;`

### **Case LINKS_PATT**
* ➖ `$x links ($I: $y)` is satisfied if `r(x) : A(r(y):r(I))` for some `A : REL(r(I))`.
* ➖ `$x links ($y)` is equivalent to `$x links ($_: $y)` for anonymous `$_` (See "Syntactic Sugar")


### **Case LINKS_LIST_PATT**
* 🔶 `$x links ($I[]: $y)` is satisfied if `r(x) : A(r(y):[r(I)])` for some `A : REL([r(I)])`.
* 🔶 `$x links ($I[]: <LIST_EXPR>)` is equivalent to `$x links ($I[]: $_y); $_y == <LIST_EXPR>;` for anonymous `$_y`

### **Case DIRECT_LINKS_PATT**
* 🔮 `$x links! ($I: $y)` is satisfied if `r(x) :! A(r(y):r(I))` for some `A : REL(r(I))`.
* 🔮 `$x links! ($I[]: $y)` is satisfied if `r(x) :! A(r(y):[r(I)])` for some `A : REL([r(I)])`.

### **Case HAS_PATT**
* ➖ `$x has $B $y` is satisfied if `r(y) : r(B)(r(x):r(B).O)` for some `r(B) : ATT`.
* ➖ `$x has $B == <VAL_EXPR>` is equivalent to `$x has $B $_y; $_y == <VAL_EXPR>` for anonymous `$_y` (see "Expressions")
* ➖ `$x has $B <NV_VAL_EXPR>` is equivalent to  `$x has $B == <NV_VAL_EXPR>` (see "Expressions"; `NV_EXPR` is a "non-variable expression")
* ➖ `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`

_Remark_. Note that `$x has $B $y` will match the individual list elements of list attributes (e.g. when `r(x) : A` and `A <! B.O`).

### **Case HAS_LIST_PATT**

* 🔶 `$x has $B[] $y` is satisfied if `r(y) : [r(B)](r(x):r(B)[].O)` for some `r(B) : ATT`.
* 🔶 `$x has $B[] == <LIST_EXPR>` is equivalent to `$x has $B[] $_y; $_y == <LIST_EXPR>` for anonymous `$_y`
* 🔶 `$x has $B[] <NV_LIST_EXPR>`is equivalent to  `$x has $B[] == <NV_VAL_EXPR>`.

### **Case DIRECT_HAS_PATT**

* 🔮 `$x has! $B $y` is satisfied if `r(y) :! r(B)(r(x):r(B).O)` for some `r(B) : ATT`.
* 🔮 `$x has! $B[] $y` is satisfied if `r(y) :! [r(B)](r(x):r(B)[].O)` for some `r(B) : ATT`.

### **Case IS_PATT**
* ➖`$x is $y` is satisfied if:
    * `$x` and `$y` are both **ivars** and: `r(x), r(y) :! A` and `r(x) = r(y)` for `A : ERA`
    * `$x` and `$y` are both **lvars** and: `r(x), r(y) : [A]` and `r(x) = r(y)` for (sum type) `A = \sum_i A_i`, `A_i : ERA` (**#BDD**)
    * `$x` and `$y` are both **tvars** and: `r(x), r(y) : ERA` and `r(x) = r(y)` (**#BDD**)

_System property_

1. ➖In the `is` pattern, neither left nor right variables are **not bound**.

_Remark_: In the `is` pattern we cannot syntactically distinguish whether we are in the "type" or "element" case (it's the only such pattern where tvars and evars can be in the same position!) but this is alleviated by the pattern being non-binding, i.e. we require further statements which bind these variables, which then determines them to be tvars are evars.

## ... Expression and list statements

### Expressions grammar

_Expression composition_

```javascript
BOOL        ::= VAR | bool                                     // VAR = variable
INT         ::= VAR | long | ( INT ) | INT (+|-|*|/|%) INT 
                | (ceil|floor|round)( DBL ) | abs( INT ) | len( T_LIST )
                | (max|min) ( INT ,..., INT )
DBL         ::= VAR | double | ( DBL ) | DBL (+|-|*|/) DBL 
                | (max|min) ( DBL ,..., DBL ) |                
DEC         ::= VAR | dec | ...                                // similar to DBL
STRING      ::= VAR | string | string + string
DUR         ::= VAR | time | DUR (+|-) DUR 
DATE        ::= VAR | datetime | DATETIME (+|-) DUR 
DATETIME    ::= VAR | datetime | DATETIME (+|-) DUR 
PRIM        ::= <any-expr-above>
STRUCT      ::= VAR | { <COMP>: (value|VAR|STRUCT), ... }      // <COMP> = struct component
                | <HINT> { <COMP>: (value|VAR|STRUCT), ... }   // <HINT> = struct label
DESTRUCT    ::= { T_COMP: (VAR|VAR?|DESTRUCT), ... }           
VAL         ::= PRIM | STRUCT |                   
<T>         ::= <T> | <T>_LIST [ INT ]                         // T : Val
                | <T_FCALL>                                    // fun call returning T/T?
                | STRUCT.<T_COMP>                              // component of type T/T?
<T>_LIST    ::= VAR | [ <T> ,..., <T> ] | <T>_LIST + <T>_LIST  // includes empty list []
                T_LIST [ INT .. INT ]
INT_LIST    ::= INT_LIST | [ INT .. INT ]
VAL_EXPR    ::= <T> | STRUCT                                   // "value expression"
LIST_EXPR   ::= <T>_LIST | INT_LIST                            // "list expression"
EXPR        ::= VAL_EXPR | LIST_EXPR
```

As a special case, consider expression that are not single variables (i.e., exclude `$x` but allow `($x + 1)` or even `($x)`)
```
NV_VAL_EXPR ::= VAL_EXPR - VAR                                 // exclude sole variable
NV_LIST_EXPR::= LIST_EXPR - VAR                                // exclude sole variable
NV_EXPR     ::= EXPR - VAR                                     
```

_Value formats_

```
  long       ::=   (0..9)*
  double     ::=   (0..9)*\.(0..9)+
  dec        ::=   (0..9)*\.(0..9)+dec
  date       ::=   ___Y__M__D
  datetime   ::=   ___Y__M__DT__h__m__s
                 | ___Y__M__DT__h__m__s:___
  datetimetz ::= ...
  duration   ::=   P___Y__M__D              
                 | P___Y__M__DT__h__m__s
                 | P___Y__M__DT__h__m__s:___
```

_Remarks_

* Careful: the generic case `<T>` modify earlier parts of the
* `T`-functions (`T_FUN`) are function calls to *single-return* functions with non-tupled output type `T` or `T?`

_Explicit casts_. 🔮 Introduce explicit castings between types to our grammar. For example:
* `long(1.0) == 1`
* `double(10) / double(3) == 3.3333`)

### Expression evaluation

Given a crow `r` that assign all vars in an `<EXPR>` we define
* value evaluation `vev@r(<EXPR>)` (math. notation `v_r(expr)`)
* type evaluation `Tev@r(<EXPR>)` (math. notation `T_r(expr)`)]

as follows. First note that we can unambiguously distinguish **value** from **list** expressions in our grammar. We evaluate each of those as follows.

#### Value expressions

* 🔶 The _value expressions_ `VAL_EXPR` is evaluated as follows:
    * **Substitute** all vars `$x` by `r($x)`
    * If `r($x)` isa attribute instance, **replace** by `val(r($x))`
    * `v_r(expr)` is the result of evaluating all operations with their **usual semantics**
        * `1 + 1 == 2`
        * `10 / 3 == 3` (integer division satisfies `p/q + p%q = p`)
    * `T_r(expr)` is the **unique type** of the substituted expression, noting:
        * We allow **implicit casts** of `long -> dec -> double`.
        * This is always unique except possibly for the `STRUCT` case (see property below)!

_System property_.

* 🔶 If `T_r(expr)` is non-unique for a `STRUCT` expression (which may be the case because, `STRUCT` may share fields) we require the expression to have a `HINT`, or otherwise throw an error (***see Grammar above***, case `STRUCT`).

_Remark_. Struct values are semantically considered up to reordering their components.

#### List expressions

* 🔶 The _list expressions_ `LIST_EXPR` is evaluated as follows:
    * Substitute all vars `$x` by `r($x)`
    * (**Do not replace** attributes!)
    * `v_r(expr)` is the result of concatenation and sublist operations with their **usual semantics**
        * e.g. `[a] + [a,b,c][1..2] = [a,b,c]` (`[1..2]` includes indices `[1,2]`)
        * or `([a] + [a,b,c])[1..2] = [a,b]`
    * `T_r(expr)` is the **minimal type** of all the list elements (usually some sum type)

**Note**: While the type checker cannot statically determine `T_r(expr)`, it can statically construct an upper bound of that type.

### (Feature) Boundedness of variables in expressions

1. 🔶 Generally, variables in expressions `<EXPR>` are **never bound**, except ...
    * 🔶 the exception are **single-variable list indices**, i.e. `$list[$index]`; in this case `$index` is bound. (This makes sense, since `$list` must be bound elsewhere, and then `$index` is bound to range over the length of the list) (**#BDD**)
3. 🔶 Struct components are considered to be unordered: i.e., `{ x: $x, y: $y}` is equal to `{ y: $y, x: $x }`.

_Remark_: The exception for list indices is mainly for convenience. Indeed, you could always explicitly bind `$index` with the pattern `$index in [0..len($list)-1];`. See "Case **LET_IN_LIST_PATT**" below.

### Simple expression patterns

#### **Case LET_PATT**
* ➖`let $x = <EXPR>` is satisfied if **both**
    * `r($x)` equals `v_r(expr)`
    * `T($x)` equals `T_r(expr)`

_System property_

1. _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Assign once, to vars only_. Any variable can be assigned only once within a pattern—importantly, the left hand side _must be_ a variable (replacing it with a concept will throw an error; this implicitly applies to "Match semantics").
3. _Acyclicity (pattern-level constraint)_. All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic (e.g. we cannot have `$x = $x + $y; $y = $y - $x;`)

#### **Case LET_DESTRUCT_PATT**
* 🔶 `let <DESTRUCT> = <STRUCT>` is satisfied if, after substituting concepts from `r`, the left hand side (up to potentially omitting components whose variables are marked as optional) matched the structure of the right and side, and each variable on the left matches the evaluated expression of the correponding position on the right.

_System property_

1. 🔶 _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Acyclicity (pattern-level constraint)_. All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic.

#### **Case EQ_PATT**
* ➖ `<EXPR1> == <EXPR2>` is satisfied if `v_r(expr_1) = v_r(expr_2)`
* ➖ `<EXPR1> != <EXPR2>` is equivalent to `not { <EXPR1> != <EXPR2> }` (see "Patterns")

_System property_

1. All variables are bound **not bound**.

#### **Case COMP_PATT**

The following are all kind of obvious (for `<COMP>` one of `<`,`<=`,`>`,`>=`):

* ➖ `<INT> <COMP> <INT>`
* ➖ `<BOOl> <COMP> <BOOL>` (`false`<`true`)
* ➖ `<STRING> <COMP> <STRING>` (lexicographic comparison)
* ➖ `<DATETIME> <COMP> <DATETIME>` (usual datetime order)
* ➖ `<TIME> <COMP> <TIME>` (usual time order)
* ➖ `<STRING> contains <STRING>`
* ➖`<STRING> like <REGEX>` (where `<REGEX>` is a regex string without variables)

_System property_

1. In all the above patterns all variables are **not bound**.

### List expression patterns

### **Case LET_IN_LIST_PATT**
* ➖`let $x in $l` is satisfied if `r(x) in r(l)`
* 🔶 `let $x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar")

_System property_

1. The right-hand side variable(s) of the pattern are **not bound**. (The left-hand side variable is bound.)
1. _Acyclicity (pattern-level constraint)_.  All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic.

### **Case LIST_CONTAINS_PATT**
* 🔶 `$l contains $x` is satisfied if `r(x) in r(l)`
* 🔶 `<LIST_EXPR contains $x` is equivalent to `$l = <LIST_EXPR>; $l contains $x` (see "Syntactic Sugar")

_System property_

1. _Boundedness_ no side of the patterns is binding (this is in effect a comparator).


# Data manipulation language

## Match semantics

* _Syntax_ The `match` clause has syntax
    ```
    match <PATTERN>
    ```

* _Input crows_: The clause can take as input a stream `{ r }` of concept rows `r`.

* _Output crows_: For each `r`:
    * replace all patterns in `PATT` with concepts from `r`.
    * Compute the **set** of answers `{ r' }`.
    * The final output stream will be `{ (r,r') }`.


## Function semantics

### Function signature

#### **Case SIGNATURE_STREAM_FUN**

* 🔶 Stream-return function signature syntax:
  _Syntax_:
    ```
    fun F ($x: A, $y: B[]) -> { C, D[], E? } :
    ```
  where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_Remark: see GH issue on trais (Could have `A | B | plays C` input types)._

_Terminology_ If the function returns a single types (`{ C }`) then we call it a **singleton** function.

#### **Case SIGNATURE_SINGLE_FUN**

* 🔶 Single-return function signature syntax:
    ```
    fun F ($x: A, $y: B[]) -> C, D[], E? :
    ```
  where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_STICKY: allow types to be optional in args (this extends types to sum types, trait types, etc.)_

_Terminology_ If the function returns a single types (`C`) then we call it a **singleton** function.

### Function body

#### **Case READ_PIPELINE_FUN**

* 🔶 Function body syntax:
  _Syntax_:
    ```
    <READ_PIPELINE>
    ```

_System property_

* 🔶 _Read only_. Pipeline must be read-only, i.e. cannot use write clauses (`insert`, `delete`, `update`, `put`)
* 🔶 _Require crow stream output_ Pipeline must be non-terminal (e.g. cannot end in `fetch`).

### Function return

#### **Case RETURN_STREAM_FUN**

The return clause corresponds to its output type signature.

* 🔶 When function output type is `{ A, B, ... }` then return clause of the form:
    ```
    return { $a, $b, ... };
    ```

_System property_

* 🔶 _Require bindings_ all vars (`$x`, `$y`, ...) must be bound in the pipeline (taken into account any variable selections through `select` and `reduce` operators).

#### **Case RETURN_SINGLE_FUN**

* 🔶 When function output type is `A, B?, ... ` then return clause of the form:
  _Syntax_:
    ```
    return <SINGLE> $x, $y, ...;
    ```
  where `<SINGLE>` can be:
    * `first`
    * `last`
    * `random`

_System property_.

* 🔶 `?` on output types correspond exactly to those variables in the return clause that are optional in the function body.
* 🔶 _Require bindings_ all vars (`$x`, `$y`, ...) must be bound in the read pipeline of the function body (taken into account any variable selections through `select` and `reduce` operators).

#### **Case RETURN_AGG_SINGLE_FUN**

* 🔶 When function output type is `A?, B?, ... ` then return clause of the form:
    ```
    return <AGG>, <AGG>, ...;
    ```
  is short-hand for:
    ```
    reduce $_1? = <AGG>, $_2? = <AGG>, ... ;
    return first $_1, $_2, ... ;
    ```

_Remark_
Note the optionality `?`, which ensures that the reduce step will not yield an empty crow stream.

### (Theory) Function semantics

***Typing***

* `?` marks variables in the row that can be optional
    * this applies both to output types: `{ A?, B }` and `A?, B`
    * and it applies to variables assignments: `$x?, $y in ...` and `$x?, $y = ...`
* **single-row returning** functions return 0 or 1 rows
* **multi-row returning** ("stream-returning") functions return 0 or more rows.

***Matching***

* single row assignment `$x, $y = ...`  fails if no row is assigned
* single row assignment `$x?, $y = ...`  succeeds if a row is assigned, even if it may be missing the optional variables
* multi row assignment `$x, $y in ...`  fails if no row is assigned
* multi row assignment `$x?, $y in ...`  succeeds if one or more row are assigned, even if they may be missing the optional variables

***Evaluation***

* A function `F` counts as ***evaluated*** on a call `F_CALL` when we completely computed its output stream as follows:
    1. provide input arguments from the call `F_CALL` as a single crow, which is the starting point of the body `READ_PIPELINE`
    2. Then act like an ordinary pipeline, outputting a stream (see "Pipelines")
    3. perform `return` transformation outlined below for final output which is effectively a `select`.
        4. For **single(-row) return** functions we first pick the **first**, **last** or a **random** crow in the stream, making the final output an at-most-single-row output
    4. Denote the output stream by `ev(F_CALL)`
* When negations are use, function in lower strata must be evaluated (on all relevant calls) before function in higher strata (see below)

### (Theory) Order of execution (and recursion)

Since functions can only be called from `match` stages in pipelines, evaluation is deterministic and does not depend on any choices of execution order (i.e. which statements in the pattern we retrieve first), except for **negation**: here, execution order _does_ matter.

* 🔮 **Recursion** Functions can be called recursively, as long as negation can be **stratified**:

    * The set of all defined functions is divided into groups called "strata" which are ordered
    * If a function `F` calls a function `G` if must be a in an equal or higher stratum. Moreover, if `G` appears behind an odd number of `not { ... }` in the body of `F`, then `F` must be in a strictly higher stratum.

  _Note_: The semantics in this case is computed "stratum by stratum" from lower strata to higher strata. New facts in our type systems (`t : T`) are derived in a bottom-up fashion for each stratum separately.

# Functions

## Terminology

* **scalar function**: returns a concept 1-tuple. signature: `-> A`
* **tuple function**: returns a concept n-tuple, n > 1. signature `-> A, B, C`
* **stream function**: returns a stream of concept tuples. signature `-> { A, ... }`
  * **scalar stream function**: returns a stream of concept 1-tuples. signature `-> { A }`
  * **tuple stream function**: returns a stream of concept n-tuples, n > 1. signature `-> { A, B, C}`

## Syntax

### Function signature

* 🔶 **Stream function** signature syntax:
  _Syntax_:
    ```
    fun F ($x: A, $y: B[]) -> { C, D[], E? } :
    ```
  where
  * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

* 🔶 **Scalar and tuple** function signature syntax:
    ```
    fun F ($x: A, $y: B[]) -> C, D[], E? :
    ```
  where
  * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

### Function body

* 🔶 Function body syntax:
  _Syntax_:
    ```
    <READ_PIPELINE>
    ```
### Function return

* 🔶 For **stream function** return clause of the form:
    ```
    return { $a, $b, ... };
    ```

* 🔶 For **scalar and tuple function** return clause of the form:
  * Case of **stream selection**
      ```
      return SINGLE $x, $y, ...;
      ```
      where `SINGLE` can be:
    * `first`
    * `last`
    * `random`
  * Case of **stream aggregation**
      ```
      return AGG $x, AGG $y, ...;
      ```
    where `AGG` can be:
    * `sum`
    * `median`
    * `count`

## Semantics

### Function returns

* `?` marks variables in the row that can be optional, meaning they could be assigned to the **empty** value `()`
    * this applies both to output types: `{ A?, B }` and `A?, B`
    * and it applies to variables assignments: `$x?, $y in ...` and `$x?, $y = ...`
* **single-row** (i.e. scalar or tuple) returning functions return 0 or 1 rows
* **multi-row** (i.e. stream) functions return 0 or more rows.

### Function calls

* a single row assignment `let $x, $y = ...`  fails if no row is assigned
* a single row assignment `let $x?, $y = ...`  succeeds if a row is assigned, even if it may be missing the optional variables
* a multi row assignment `let $x, $y in ...`  fails if no row is assigned
* a multi row assignment `let $x?, $y in ...`  succeeds if one or more row are assigned, even if they may be missing the optional variables

### Function evaluation

See [read spec](read.md).


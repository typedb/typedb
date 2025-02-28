# Functions

Function definition

## Returns
TODO: scalar/tuple/stream terminology

* `?` marks variables in the row that can be optional
    * this applies both to output types: `{ A?, B }` and `A?, B`
    * and it applies to variables assignments: `$x?, $y in ...` and `$x?, $y = ...`
* single-row returning functions return 0 or 1 rows
* multi-row returning ("stream-returning") functions return 0 or more rows.
* and single row assignment `$x, $y = ...`  fails if no row is assigned
* and single row assignment `$x?, $y = ...`  succeeds if a row is assigned, even if it may be missing the optional variables
* and multi row assignment `$x, $y in ...`  fails if no row is assigned
* and multi row assignment `$x?, $y in ...`  succeeds if one or more row are assigned, even if they may be missing the optional variables

## Syntax

return `first` `last`
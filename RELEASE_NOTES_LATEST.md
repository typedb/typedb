Install & Run: https://typedb.com/docs/home/install


## New Features
- **Is constraint**
  
  We implement the `is` constraint support in queries:
  
  ```php
  match
      $x isa company;
      $y isa person;
      $z isa person;
      not { $y is $z; };  # <---
      $r1 isa employment, links ($x, $y);
      $r2 isa employment, links ($x, $z);
      select $x, $y, $z;
  ```
- **Introduce 3.0 undefine queries**
  We introduce `undefine` queries with the new "targeted" syntax to enable safe and transparent schema concept undefinitions.

If you want to undefine the whole type, you can just say:
```
undefine 
  person;
```

If you want to undefine a capability from **somewhere**, use `undefine **capability** from **somewhere**`.
It consistently works with `owns` (`person` is preserved, only the `owns` is undefined):
```
undefine
  owns name from person;
```
annotations of `owns` (`person owns name` is preserved, only `@regex(...)` (you don't need to specify the regex's argument) is undefined):
```
undefine
  @regex from person owns name;
```
and any other capability, even specialisation:
```
undefine
  as parent from fathership relates father;
```

Want to undefine multiple concepts in one go? Worry not!
```
undefine
  person;
  relates employee from employment;
  @regex from name;
  @values from email;
```

The error messages in all definition queries (`define`, `redefine`, and `undefine`) were enhanced, and the respective `query/language` BDD tests were introduced in the CI with `concept` unit tests.

Additional fixes:
* answers of `match` capability queries like `match $x owns $y`, `match $x relates $y`, and `$match $x plays $y` now include transitive capabilities;
* `define` lets you write multiple declarations of an undefined type, specifying its kind **anywhere**, even in the last mention of this type;
* failures in schema queries for schema transactions no longer lead to freezes of the following opened transactions;
* no more crashes on long string attributes deletion with existing `has` edges;




## Bugs Fixed
- **Fix outputs from earlier disjunction branches affecting later branches**
  Fixes outputs from earlier disjunction branches affecting later branches
  
  
- **Ensure sorted join executor respects selected outputs**
  
  Before this change, an intersection executor (which is used for any number of sorted iterators, not necessarily a join) would write into the output row all items in the constituent executors, so long as they have a row position assigned. That would happen even if those values were not selected for future steps, which prevented deduplication of answers from being handled correctly.
  
  

## Code Refactors
- **Simplify attribute encoding**
  
  We remove separate prefixes for attribute instances and instead introduce a value type prefix after the attribute type encoding.
  
  Encoding before:
  ```
  [String-Attribute-Instance][<type-id>][<value>][<length>]
  ```
  After: 
  ```
  [Attribute-Instance][<type-id>][String][<value>][<length>]
  ```
  
  **Note: this change breaks backwards compatibility**
  
  
- **Refactor after basic functions**
  Refactors the executor to be have more simple, flat instructions than less, complex ones. 
  
  

## Other Improvements
 
- **Carry correct types into type list executor**
  
  In cases where the list of types is dependent on the context (e.g. `{$t label T;} or {$t label U;};`, the type list executor (role name, explicit label, or kind) should only be producing the types in the list defined by the constraint. That was handled correctly when the type constraint was used as a post-check, but incorrectly during direct execution.
  
  
- **Update spec.md**

- **Introduce 3.0 `value` and `as` match constraints**
  We introduce `value` match constraint for querying for attribute types of specific value types:
  ```
  match
    $lo value long;
  ```
  
  We introduce `as` match constraint for querying for role types specialisation:
  ```
  match
    $relation relates $role as parentship:child;
  ```
  which is equivalent to:
  ```
  match
    $relation relates $role;
    $role sub parentship:child;
  ```
  but is useful for shortening your queries, making it similar to `define` definitions, and in case of anonymous variables:
  ```
  match
    $relation relates $_ as parentship:child;
  ```
  
- **Optimise comparators**
  
  We optimise the execution of queries containing comparators (`>/</==/...`) where these can be used to restrict the data search space. This PR sets up general infrastructure for using restrictions when searching the data, but uses it only for optimising ISA lookups - follow up work will optimise HAS operations as well.
  
  
- **Add type annotation to fix build in circleci**

- **Update spec: fix list insert, add value pattern, add as! pattern**

- **Bugfix: do not reuse cartesian product iterators between inputs**
  
  We can't reuse the iterators created from the previous input row, as they would iterate over different tuples. Now, when we take a new input, we reset the cartesian iterators.
  
  
- **Update spec.md (fix sssnake expresssions)**

- **Green match BDD redux**
  
  We resolve the remaining flaky failures in match BDDs.
  
  
- **Update banner.png**

- **Update many unit & integration tests & enable in CI**
  Updates many unit & integration tests & enables them in CI
  
  
    

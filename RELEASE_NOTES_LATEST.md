**Download from TypeDB Package Repository:**

[Distributions for 3.4.4](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.4.4)

**Pull the Docker image:**

```docker pull typedb/typedb:3.4.4```


## New Features
- **Implement missing reducers: min/max, decimal**
  
  We implement min/max reducers for every totally ordered value type (NOTE: does not include `Duration` as it's only partially ordered). We also implement statistical reducers (mean/std/median) for `Decimal`, decaying to `Double` in `std`.
  
  
- **Implement optionality in read queries**
  
  We implement a huge new feature: optionality in read queries! This means you can now do partial matches. 
  
  ### Basics 
  In the following query, all `person` instances will be returned, and if any name exists, the name will be added to the answer. However, _not_ having an answer will _not_ cause that person instance to be eliminated:
  ```
  # Find any person, and if they have a name add that into the answer
  match 
    $p isa person;
    try { $p has name $name; };
  ```
  The answers will conceptually conform to a type like `[ $p=person, $name=Option<name>]`.
  
  Optionals only return when the _full_ pattern inside the optional matches:
  ```
  # Return any people, and use the ages only of people with name "john", else do not match anything additional
  match 
    $p isa person;
    try { $p has name "john"; $p has age $age; };
  ```
  
  Multiple `try` clauses are matched to the maximum degree at simultaneously for each non-optional answer component:
  ```
  # If every person can have at most 1 name and 1 age: return every person, plus their name if it exists, plus their age if it exists
  match
    $p isa person;
    try { $p has name $name; };
    try { $p has age $age; };
  ```
  Note that this will produce 1 answer per person, not a permutation of each person, with and without their names and ages. If there are higher cardinalities of name or age, you get permutations of each possible name and age filled but neither filled.
  
  Optional variables cannot be re-used across sibling clauses due to ambiguous outcomes:
  ```
  ### ILLEGAL - optional $name cannot be re-used across sibling try clauses
  match 
    $p isa person;
    try { $p has name $name; }; 
    try { $x isa person, has name $name; };
  ```
  
  ### Nesting and Pipelining
  
  Optionals can also not be nested within disjunctions (`or`) or negations (`not`) - in negations, these operations would be no-ops, and disjunctions are currently limited to only returning variables that are guaranteed to be returned from any branch, ie. they cannot currently return optional values at all.
  
  Optional variables _can_ be used across stages of a query pipeline. Any pattern in a query using an optional variable that is not set will "fail" immediately (return 0 answers). However, by nesting a pattern containing an unset optional variable inside try` clause, this behaviour is absorbed since only the `try` clause will fail:
  
  ```
  match
    try { $name isa name "John"; };
  match
    try { $p isa person, has name $name; };
  
  ```
  This will return exactly `[$name=None, $p=None ]` or `[$name=Some(name), $p=Some(person) ]` or `[$name=Some(name), $p=None]`
  
  In contrast, by using the optional `$name` _outside_ any optional scope, the following query will eliminate any answers that don't have `$name` set and only return answers containing `[$name=name, $person=person]`
  ```
  match
    try { $name isa name "John"; };
  match
    $p isa person, has name $name;
  ```
  
  
  

## Bugs Fixed
- **Only seek on iterators that are behind in merge**
  
  Fix a crash when during a seek on a `KMergeBy`, which is merging multiple sorted tuple iterators, `seek` is called on iterators that are already ahead of the seek target, an invalid operation. It is valid to `seek` the iterator as a whole, as the merge may still be behind the seek point even if some constituent iterators are ahead.
  
  

## Code Refactors

- **Separate out optional & negation type inference**
  Since negated & optional patterns cannot influence the type-annotations of the containing conjunction, we need not interleave their type-inference  with that of the parent conjunction. Hence, We refactor the type-inference procedure so that type-inference on a conjunction & its disjunctions is completed before we begin the process on negated & optional subpatterns.


## Other Improvements
- **Update warning printing on bootup**

- **Update behaviour repository and make step checking more strict**

    

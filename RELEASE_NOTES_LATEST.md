Install & Run: https://typedb.com/docs/home/install


## New Features
- **Allow disconnected relations**
  
  We implement the suggestion from https://github.com/vaticle/typedb/issues/6920, which will allow relations to exist without role players for the duration of a write transaction. At the end of the transaction, any relations without role players are deleted (preserving the existing behaviour of automatically deleting relations without role players, but we delay the operation until commit time).
  
  This change addresses https://github.com/vaticle/typedb/issues/6902, https://github.com/vaticle/typedb/issues/6920, 
  
  
- **Implement Fetch query**
  
  We implement the language specification for TypeQL "Fetch" queries, finalised and implemented at https://github.com/vaticle/typeql/pull/300. 
  
  This change adds a new embedded and network API: `QueryManager.fetch()`. This API accespts TypeQL Fetch queries, and returns JSON-like structures. 
  
  **Changes to TypeQL Get queries**
  As part of this change, we have updated the existing terminology around "TypeQL Match" queries to be exclusively called "TypeQL Get" queries. TypeQL Get queries now require a 'get' clause be written at the end of queries. For example:
  
  ```
  match
  $x isa person;
  ```
  is no longer a valid query and must be updated to:
  ```
  match
  $x isa person;
  get;                  # alternatively 'get $x;' is more precise
  ```
  
  An empty 'get' clause with no variables will return all variables from the 'match' clause, exactly as the previous behaviour which specified no get clause at all.
  
  **Modifiers on write queries**
  We now also allow modifiers on any kind of query, including write queries, that uses a 'match' clause, by adding the desired `sort`, `offset`, or `limit` to the end of your queries. For example, we now allow:
  ```
  match
  $x isa person, has age $a;
  delete
  $x has $a;
  sort $a; limit 10;
  ```
  
  
  

## Bugs Fixed
- **Fix traversing edges to role vertices**
  Fixes a bug in resolving edge types when traversing to a role vertex.
  This could cause missing results in queries involving role variables and relations playing roles in relations.

- **Fix docker container image workdir to include architecture suffix**
  

## Code Refactors
- **Bundle java deps ignoring transitive dependencies without maven coordinates**
  
  Recent upgrades to bazel build rules for java, protobuf, and grpc meant that their internal dependencies no longer use Maven coordinates, but are more bazel-native in their build process. However, this means that to 'correctly' bundle TypeDB Server, we also need to include the rules' dependencies into our distribution (such as `com_google_protobuf`). The issue arises that we also directly use these libraries occasionally, and pull them via Maven. As a result, we end up with duplicates: bazel-build JARs, and maven-sourced JARs inserted into our distribution.
  
  Our workaround for now is to skip all bazel-build JARs that don't also have maven coordinates. This is **dangerous** since it means we don't find out we have missing JARs in our distributions until booting up the server and testing it. This works only because we have manually verified to include all the 'equivalent' JARs from maven in our distribution instead.
  
  Our reasoning for this is that in TypeDB 3.0 (rust) this issue will no longer be present as most dependencies will be compiled into the final static library.
  
  
  
- **Update reasoner benchmark to avoid role aliases**
  Updates the IAM reasoner benchmark schema to account for role aliases being disallowed. Relaxes some of the counter upper bounds to accommodate the resulting changes in the plans.
  
  
- **Disallow explicit use of role type aliases**
  
  We disallow using role type aliases in labels, only permitting their use when querying for role types in relation constraints. This finalises the implementation of behaviours outlined in https://github.com/vaticle/typeql/issues/274#issuecomment-1766214447.
  
  Given
  ```
  define
  employment sub relation, relates employee;
  part-time-employment sub employment;
  ```
  We now disallow
  ```
  define
  person plays part-time-employment:employee;
  ```
  As this role type is an alias for `employment:employee` and therefore generally used incorrectly/confusing.
  
  However, matching is still flexibly allowed:
  ```
  match
  (employee: $x) isa part-time-employment;
  ```
  
  

## Other Improvements

- **Apt deployment depends on default-jre instead of openjdk-11-jre to be compatible with more versions**
  
  
- **Update README file**
  
  Update the README file.
  
  
    

Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.28.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.28.0)

Server + Console: [Distributions for 2.28.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.28.0)


## New Features
- **Use Rosetta for Intel Mac jobs in Circle CI**
  
  CircleCI is sunsetting its MacOS Intel architecture executors. This PR transitions our Mac x86_64 CI tests to run on ARM executors using Rosetta.
  
- **Sentry transaction**
  
  We reintroduce a simple notification task, when diagnostics is enabled, which notifies the diagnostics server of the server ID.
  

## Bugs Fixed
- **Fix application of query bounds through match-fetch queries**
  
  We fix a bug that was revealed when using `match-fetch` queries with subqueries that had nested patterns. This change now correctly applies 1) filtering to a parent match query so that the right outputs are generated 2) passes the bounds received from a preceding query correctly into all child patterns of subsequent query clauses.
  
  
- **Check supertypes of relation when inferring player role types during insert**
  
  Given the following minimal schema:
  ```
  define
  player sub entity, plays super-relation:super-role;
  super-relation sub relation, relates super-role;
  sub-relation sub super-relation;
  ```
  
  It was impossible to insert an instance of `sub-relation` relating a `player` without explicitly specifying its role:
  ```
  > match $player isa player; 
    insert ($player) isa sub-relation; 
  [THW27] Invalid Thing Write: Unable to add role player '$player' to the relation, 
          as there is no provided or inferrable role type.
  ```
  
  The reason for this was that during the handling of the `insert` query, we check if it can play a role defined directly on the requested relation type, without considering any role types it may have inherited. This PR resolves that issue.
  
  

## Code Refactors
- **Disable using type information from IID in type inference**
  
  We decide to ignore IID information when performing type inference. This comes from the idea that IIDs are _data_, and the user may be trying to check whether the data conforms to a specific part of the schema. Previously, this will cause a query to throw an error due to type checking incompatibility, which made common expressions in fetch subqueries very difficult
  
  Fetching stocks for books that are able to have stock (some subtypes of books do not have stock, such as 'ebook'):
  ```
  match
  $x isa book;
  fetch:
  stock: { 
    match $x isa! $t; $t owns stock;
    fetch $x: stock;
  };
  ```
  
  Which will retrieve stock for books, if the polymorphically selected book instance is able to own 'stock' attributes.
  
  This expression now returns an empty list for instances of books that cannot own stocks instead of returning an error.
  
  

## Other Improvements
  
- **Update protocol dependency**

- **Update bat file license headers**

- **Replace licenses with MPL version 2.0**

- **Update windows choco dependencies**

    

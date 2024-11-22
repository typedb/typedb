Install & Run: https://typedb.com/docs/home/install


## New Features
- **Exit on panic**
  
  In Rust, whenever a panic is encountered in a background thread, the main thread is normally unaffected until it either joins the background thread, or attempts to lock a mutex that was held by the panicking thread. If that communication never occurs (e.g. no locks were held by a worker), the server may end up in an invalid state.
  
  We override the default behaviour to `exit` from the process with code `1`.

- **Stabilise fetch and introduce fetch functions**
  We introduce function invocation in `fetch` queries. Fetch can call already existing functions:
  ```
  match
    $p isa person;
  fetch {
    "names": [ get_names($p) ],
    "age": get_age($p)
  };
  ``` 
  and also use local function blocks:
  ```
  match
    $p isa person;
  fetch {
    "names": [
      match
        $p has name $n;
        return { $n };
    ],
    "age": (
      match
        $p has age $a;
        return first $a;
    )
  };
  ```
  In the examples above, results collected in concept document lists (`"names": [ ... ]`) represent streams (purposely multiple answers), while single (optional) results (`"age": ...`) represent a single concept document leaf and do not require (although allow) any wrapping.


Moreover, we stabilize attribute fetching, allowing you to expect a specific concept document structure based on your schema. This way, if the attribute type is owned with default or key cardinality (`@card(0..1)` or `@card(1..1)`), meaning that there can be at most one attribute of this type, it can be fetched as a single leaf, while other cardinalities force list representation and make your system safe and consistent. For example, a query
  ```
  match
    $p isa person;
  fetch {
    $p.*
  };
  ```
can automatically produce a document like
  ```
  {
    "names": [ "Linus", "Torvalds" ],
    "age": 54
  }
  ```
with
  ```
  define
    entity person 
      owns name @card(1..), 
      owns age @card(1..1);
  ```

Additionally, fetch now returns attributes of all subtypes `x sub name; y sub name;` for `$p.name`, just like regular `match` queries like `match $p owns name $n`.

With this, feel free to construct your fetch statements the way you want:
  ```
  match
    $p isa person, has name $name;
    fetch {
      "info": {
        "name": {
          "from entity": [ $p.person-name ],
          "from var": $name,
        },
        "optional age": $p.age,
        "rating": calculate_rating($p)
      }
    };
  ```
to expect consistent structure of the results:
  ```
  [
    {
      "info": {
        "name": {
          "from entity": [ "Name1", "Surname1" ],
          "from var": "Name1"
        },
        "optional age": 25,
        "rating": 19.5
      }
    },
    {
      "info": {
        "name": {
          "from entity": [ "Name1", "Surname1" ],
          "from var": "Surname1"
        },
        "optional age": 25,
        "rating": 19.5
      }
    },
    {
      "info": {
        "name": {
          "from entity": [ "Bob" ],
          "from var": "Bob"
        },
        "optional age": null
        "rating": 28.783
      }
    }
  ]
  ```


- **Implement query executable cache**

  We implement the cache (somewhat arbitrarily limited to 100 entries) for compiled executable queries, along with cache maintanance when statistics change significantly or the schema updates.

  Query execution without any cache hits still looks like this:
  ```
  Parsing -> Translation (to intermediate representation) -> Annotation -> Compilation -> Execution
  ```
  However, with a cache hit, we now have:
  ```
  Parsing -> Translation ---Cache--> Execution
  ```
  skipping the annotation and compilation/planning phases, which take significant time.

  Note that schema transactions don't have a query executable cache, since keeping the cache in-sync when schema operations run can be error prone.

  The query cache is a _structural_ cache, which means it will ignore all Parameters in the query: variable names, constants and values, and fetch document keys. Most production systems run a limited set of query structures, only varying values and terms, making a structural cache like this highly effective!


- **Introduce naive retries for suspended function calls**
  Introduces retries for suspended function calls. 
  Functions may suspend to break cycles of recursion. Restoring suspend points is essential to completeness of recursive functions, and thus correctness of negations which call them.


- **Query execution analyser**

  We implement a useful debugging feature - a query analyzer. This is similar to Postgres's `Explain Analyze`, which produces both the query plan plus some details about the data that has flowed through the query plan and the time at each step within it.

  Example output:
  ```
  Query profile[measurements_enabled=true]
    -----
    Stage or Pattern [id=0] - Match
      0. Sorted Iterator Intersection [bound_vars=[], output_size=1, sort_by=p0]
        [p0 isa ITEM] filter [] with (outputs=p0, )
      ==> batches: 158, rows: 10000, micros: 6407
  
      1. Sorted Iterator Intersection [bound_vars=[p0], output_size=2, sort_by=p1]
        Reverse[p1 rp p0 (role: __$2__)] filter [] with (inputs=p0, outputs=p1, checks=__$2__, )
      ==> batches: 854, rows: 39967, micros: 75716
  
    -----
    Stage or Pattern [id=1] - Reduce
      0. Reduction
      ==> batches: 1, rows: 10000, micros: 116035
  
    -----
    Stage or Pattern [id=2] - Insert
      0. Put attribute
      ==> batches: 10000, rows: 10000, micros: 5890
  
      1. Put has
      ==> batches: 10000, rows: 10000, micros: 54264
    ```

  When disabled, profiling is a no-op (no strings are created, locks taken, or times measured), though there is still some cost associated with cloning Arcs containing the profiling data structures around.

  To enable query profiling, the easiest way (for now) is to enable TRACE logging level for the `executor` package, currrently configured in `//common/logger/logger.rs`:
  ```
  .add_directive(LevelFilter::INFO.into())
  // add:
  // .add_directive("executor=trace".parse().unwrap())
  ```

  Alternatively, just set the `enable` boolean to `true` in the `QueryProfile::new()` constructor.


## Bugs Fixed
- **Variable position fix**

- **Disable variables of category value being narrow-able to attribute**
  Disable variables of category value being narrowed to attribute
  ```
  $name = "some name"; $person has name == $name; # Fine
  $name == "some name"; $person has name $name; # Also Fine
  $name = "some name"; $person has name $name; # Disallowed
  ```


- **Fix disjunction inputs when lowering**
  The disjunction compiler explicitly accepts a list of variables which are bound when the disjunction is evaluated. This also fixes a bug where input variables used in downstream steps would not be copied over.

- **Fixes early termination of type-inference pruning when disjunctions change internally**
  Fixes early termination of type-inference pruning when disjunctions change internally

## Code Refactors

- **Update FactoryCI images to typedb-ubuntu**

- **Rename org to 'typedb' in deployment.bzl**

- **Rename org to 'typedb' in CircleCI docker job**

- **Boxed large result Error variants**

  We follow some best practices to box the large Error variants of Result types, which should optimise both the stack size and the amount of memory that has to be copied between function calls.

- **Decimal to double cast**

  We can now use decimals in double expressions.

- **Optimise has comparators and implement Executable Display**

  We inline comparators into `HasReverse` executors, as a continuation of #7233, which inlined comparators into `Isa` executors. Both optimisations reduce the search space by using the value ranges provided by constants in the query or previously executed query steps. We also improve the query planner's statistics and cost function.



## Other Improvements

- **Add cargo check to CI**
  
  We add a job to Factory CI which verifies that the Cargo configuration is valid and buildable.
  
 
- **Return query operations errors on commit**
  We return errors from the awaited operations on `commit` as the commit error. It can be useful if you want to run a number of queries from your TypeDB Driver like:
  ```rust
  let queries = [........];
  let mut promises = vec![];
  for query in queries {
      promises.push(transaction.query(query));
  }
  
  let result = transaction.commit().await;
  println!("Commit result will contain the unresolved query's error: {}", result.unwrap_err());
  ```
  
- **Update FactoryCI images to typedb-ubuntu**

- **Fix structural equality test**

- **Implement structural equality**
  
  We implement a form of Structural Equality, which check the equality of two queries, in Intermediate Representation format, which allows us to compare to queries for equality while ignoring user-written variable names and function names and constants. This is a building block for the next step, which will introduce a executable cache, allowing TypeDB to skip type inference and planning for queries that already exist in the cache.
  
- **Deleted BUILD_java**

- **Commit generated Cargo.tomls + Cargo.lock**
  
  We commit the generated cargo manifests so that TypeDB can be built as a normal cargo project without using Bazel.
  
  Integration and unit tests can be run normally, and rust-analyzer behaves correctly. Behaviour tests currently cannot be run without Bazel, as the tests expect to find the feature files in `bazel-typedb/external/typedb_behaviour`.
  
  In addition, if during development the developer needs to depend on a local clone of a dependency (e.g. making parallel changes to typeql), the Cargo.toml would need to be temporarily manually adjusted to point to the path dependency.
  
- **Reformat import**

- **Fix test assertion**

- **Use type inference in sub constraints**
  
  Previously we would use concept API to determine super / subtypes when executing as `sub` instruction. However, that work has already been done during type inference, so it is cheaper and more correct to reuse that work.
  
- **Abstract types should never be inferred for instance variables**
  
  This PR makes makes the type checker more precise (and complain less), by excluding abstract types from being inferred for instance variables.
  
- **Reenable commented tests**

    

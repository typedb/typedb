**Download from TypeDB Package Repository:**

[Distributions for 3.2.0-rc2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.2.0-rc2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.2.0-rc2```


## New Features
- **Implement schema retrieval**
  
  We implement the (pre-existing) endpoint to retrieve the schema in two variations. These used to return an 'unimplemented' error.
  
  - get the define query that specificies the Types (on the driver, this is available via a retrieved database's `database.type_schema()`)
  - get the define query that specifies the Types and the Functions (on the driver, this is available via a retrieved database's `database.schema()`)
  
  This API is useful to export and save an existing database schema, and also in particular useful for Agentic systems which use the schema definition to generate read or write queries for a database.
  
  
- **Enable GRPC transaction and query options**
  Enable options provided by the TypeDB GRPC protocol.
  
  **Enable transaction options:**
  * `transaction_timeout`: If set, specifies a timeout for killing transactions automatically, preventing memory leaks in unclosed transactions,
  * `schema_lock_acquire_timeout`: If set, specifies how long the driver should wait if opening a transaction is blocked by an exclusive schema write lock. 
  
  **Enable query options:**
  * `include_instance_types`: If set, specifies if types should be included in instance structs returned in ConceptRow answers,
  * `prefetch_size`: If set, specifies the number of extra query responses sent before the client side has to re-request more responses. Increasing this may increase performance for queries with a huge number of answers, as it can reduce the number of network round-trips at the cost of more resources on the server side.
  
  
- **Improve performance using storage seeks and enhance profiling tools**
  
  We propagate "seekable" iterators throughout the query executors, which allows us to trade off doing more seeks for doing more advances through the rocksdb iterators. We also extend the ability to profile performance of various operations, such as query execution and compiling, and transaction commits.


- **Support query 'end' markers**

  Allow terminating queries explicitly with an 'end;' stage. This finally allows disambiguating query pipelines.

  For example, the following is by default 1 query pipeline:
  ```
  match ...
  insert ...
  match ...
  insert ...
  ```

  but can be now be split into up to 4 pipelines explicitly:
  ```
  match ...
  end;
  insert ...
  end;
  match ...
  insert ...
  end;
  ```
  Now makes 3 individual queries. Note that the TypeDB "query" endpoint still only supports 1 query being submitted at a time.


- **Add branch provenance  & query structure to HTTP response**
  Each answer to a query now includes a `provenance` field, telling us which branches were taken to derive the answer.
  The query response also includes a `queryStructure` field which describes the constraints in the query in terms of "edges" corresponding to the atomic TypeDB constraints. Conceptually, An edge is:
  ```rust
  struct QueryStructureEdge {
      type_: StructureEdgeType,
      from: StructureVertex,
      to: StructureVertex,
  }
  ```
  E.g. the statement `$x has $y`  would be
  ```rust
  QueryStructureEdge {
    type_: StructureEdgeType::Has,
    from: StructureVertex::Variable("x"),
    to: StructureVertex::Variable("y"),
  }
  ```
  By substituting the variables in each row back into the structure, we can reconstruct the "Answer graph"

  **Note:** We use a 8-bytes per answer to track the branch-provenance. If the number of disjunction branches in a query exceeds 63, the query-structure will not be returned as the branch-provenance is no longer accurate.


## Bugs Fixed
- **Execute deletes in two passes - first constraints, then concepts**
  If a delete stage which deletes both constraints and concepts, a certain row may delete a concept referenced by a connection in a subsequent row. We avoid this problem by first deleting constraints for every row in an initial pass, followed by concepts of every row in a second pass.
  
  

## Code Refactors
- **Combine multiple BDDs into single target to reduce build times**
  Combines multiple behaviour tests into a single rust target to reduce build times.
  
  We now rely on cargo's ways of specifying tests to run. Examples:
  ```
  bazel test //tests/behaviour/query:test_query --test_output=streamed --test_arg="--test-threads=1" --test_arg="test_write"
  bazel test //tests/behaviour/query:test_query --test_output=streamed --test_arg="--test-threads=1" --test_arg="functions::"
  ```
  Or
  ```
  cargo test --test test_query -- --test-threads=1 test_write
  cargo test --test test_query -- --test-threads=1 functions::
  ```
  

## Other Improvements
- **Update console artifact**

- **Add span to returned query-structure**
  Adds the span of each constraint to the returned query-structure.
  
  
- **HTTP write queries return some answers on limit & more tests**
  When the query option `answerCountLimit` is hit in write queries, the transaction service no longer returns errors. Instead, the whole query is executed, but only a limited number of answers is returned with a warning string, similar to read queries.
  
  Additionally, more behavior tests for HTTP-specific features are introduced.
  
  

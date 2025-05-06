**Download from TypeDB Package Repository:**

[Distributions for 3.2.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.2.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.2.0```


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




- **Introduce branch provenance tracking**
  Introduce branch provenance tracking. This allows us to determine which branch of a disjunction a given answer originates from. Though meaningless in the logical semantics, this is helpful for other applications like visualisations.


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

  
- **Introduce HTTP endpoint**
  Introduce TypeDB HTTP endpoint to allow web applications and programming languages without TypeDB Drivers to connect to TypeDB and perform user management, database management, and transaction management, with multiple ways of running queries.
  
  ### Configuration
  The updated configuration of TypeDB server includes:
  - the old `--server.address` option for the regular gRPC endpoint (used in TypeDB Console and other TypeDB Drivers);
  - `--server.http.enable` flag to enable or disable the new HTTP endpoint;
  - `--server.http.address` option for the new HTTP endpoint.
  **Note** that the HTTP endpoint is enabled by default. It can be disabled using the corresponding flag above.
  
  While booting up the server, a new line with connection information will be displayed:
  ```
  Running TypeDB 3.2.0.
  Serving gRPC on 0.0.0.0:1729 and HTTP on 0.0.0.0:8000.
  ```
  
  ### API
  The full description of the API can be accessed on [TypeDB Docs](https://typedb.com/docs/drivers/). It includes:
  - database management and schema retrieval through `/v1/databases`;
  - user management through `/v1/users`;
  - transaction management through `/v1/transactions` with querying through `/v1/transactions/:transaction-id/query`;
  - a single query endpoint that automatically opens and closes/commits a transaction to execute the query passed: `/v1/query`.
  
  ### Authentication
  For security and optimization purposes, both gRPC and HTTP connections use authentication tokens for request verification. To acquire a token, use:
  - `Authentication.Token.Create.Req` protobuf message for gRPC, including password credentials: `username` and `password`;
  - `/v1/signin` HTTP request with a JSON body containing `username` and `password`. The received tokens must be provided with every protected API call in the header as a `Bearer Token`.
  
  The tokens are invalidated on manual password changes and automatically based on the new configuration flag: `--server.authentication.token_ttl_seconds` (with the default value of 14400 - 4 hours).
  
  Also, note that transactions are exclusive to the users that opened them, and no other user has access to them.
  
  ### Encryption
  Encryption is set up as always and now affects not only the regular gRPC connection but also the new HTTP endpoint. If encryption is disabled, it's disabled for both endpoints. If it is enabled, its settings are used for both.
  
  ### CORS
  Currently, the default permissive (allowing all headers, methods, and origins) CORS layer is set up for the HTTP endpoint.
  
  ### Running big queries
  The first version of the HTTP endpoint does not support query answer streaming. It means that, unlike in gRPC, the query results will be fully consumed before an initial answer is received on the client side, and the whole list of concept rows or documents will be returned in a single response. 
  
  While this mechanism will be enhanced in the future, for safety purposes, please use a special query option `answerCountLimit` to limit the amount of produced results and avoid too long query execution.
  
  If this limit is hit:
  - read queries will return `206 Partial Content` with all the answers processed;
  - write queries will stop their execution with an error, and the transaction will be closed without preserving intermediate results.
  
  For example:
  1. Sending a request to `localhost:8000/v1/transactions/:transaction-id/query` with the following body:
  ```json
  {
      "query": "match $p isa person, has $n; delete has $n of $p;",
      "queryOptions": {
          "answerCountLimit": 1
      }
  }
  ```
  2. Receiving: `400 Bad Request`
  ```json
  {
      "code": "HSR13",
      "message": "[TSV17] Write query results limit (1) exceeded, and the transaction is aborted. Retry with an extended limit or break the query into multiple smaller queries to achieve the same result.\n[HSR13] Transaction error."
  }
  ```
  
## Bugs Fixed
- **Don't fetch validation data until required**
  
  We delay reading the data necessary for cardinality constraint validation until we have determined that we need to perform any validation.
  
  
- **Execute deletes in two passes - first constraints, then concepts**
  If a delete stage which deletes both constraints and concepts, a certain row may delete a concept referenced by a connection in a subsequent row. We avoid this problem by first deleting constraints for every row in an initial pass, followed by concepts of every row in a second pass.
  
  
- **Fix crashing race condition after write query**
  
  Resolve issue where a large write query (~100k keys) would sometimes cause the server to crash at the end.
  
  

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
- **Change encoding of provenance in HTTP api to be collection of branch indexes**
  Instead of using the more compact bit-array representation, the `provenanceBitArray` field for each answer is now renamed to `involvedBranches` and encoded as a list of branch indexes.
  
  The set of constraints satisfied by an answer[i] is easily computed by:
  `result.answer[i].involvedBranches.flatMap(branchIndex => result.queryStructure.branches[branchIndex])`
  
  
- **Replace 3.0 with master in CI/CD jobs**
  Replace "3.0" with "master"

- **Update console artifact**

- **Add span to returned query-structure**
  Adds the span of each constraint to the returned query-structure.
  
- **Update dependencies**
  
  
- **Simplify resolving role-types in writes**
  Refactors role-type resolution in insert, update & delete stages to be done in a single function.
  
  
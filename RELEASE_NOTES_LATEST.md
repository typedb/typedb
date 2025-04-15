**Download from TypeDB Package Repository:**

[Distributions for 3.2.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.2.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.2.0-rc0```


## New Features
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
  - database management through `/v1/databases`;
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
  
  ### Querying
  All transactions and queries are supported. The usual format of concept rows answer is the following:
  <details>
  <summary>Query answer example</summary>
  
  ```json
  {
      "queryType": "read",
      "answerType": "conceptRows",
      "answers": [
          {
              "p": {
                  "kind": "entity",
                  "iid": "0x1e00000000000000000000",
                  "type": {
                      "kind": "entityType",
                      "label": "person"
                  }
              },
              "n": {
                  "kind": "attribute",
                  "value": "John",
                  "valueType": "string",
                  "type": {
                      "kind": "attributeType",
                      "label": "name",
                      "valueType": "string"
                  }
              }
          },
          {
              "n": {
                  "kind": "attribute",
                  "value": "Bob",
                  "valueType": "string",
                  "type": {
                      "kind": "attributeType",
                      "label": "name",
                      "valueType": "string"
                  }
              },
              "p": {
                  "kind": "entity",
                  "iid": "0x1e00000000000000000001",
                  "type": {
                      "kind": "entityType",
                      "label": "person"
                  }
              }
          },
          {
              "n": {
                  "kind": "attribute",
                  "value": "Alice",
                  "valueType": "string",
                  "type": {
                      "kind": "attributeType",
                      "label": "name",
                      "valueType": "string"
                  }
              },
              "p": {
                  "kind": "entity",
                  "type": {
                      "kind": "entityType",
                      "label": "person"
                  }
              }
          }
      ],
      "comment": null
  }
  ```
  </details>
  
  The result above can be achieved in two different ways:
  
  1. Manual transaction management
  
  <details>
  <summary>Click to expand</summary>
  
  Open a transaction using a `POST` request `localhost:8000/v1/transactions/open`. 
  Provide an authorization token in the header (see *Authentication* above) and a JSON body, containing information about the target database and required transaction type:
  ```json
  {
      "databaseName": "typedb",
      "transactionType": "read"
  }
  ```
  
  If everything is correct, you will receive a reply with a body like:
  ```json
  {
      "transactionId": "e1f8583c-2a03-4aac-a260-ec186369e86f"
  }
  ```
  
  Then, send a `POST` query request to `localhost:8000/v1/transactions/e1f8583c-2a03-4aac-a260-ec186369e86f/query`, with the same authorization token in the header and the following JSON body included: 
  ```json
  {
      "query": "match $p isa person, has name $n;",
      "queryOptions": { // optional
          "includeInstanceTypes": true
      }
  }
  ``` 
  </details>
  
  
  2. Single query method
  
  <details>
  <summary>Click to expand</summary>
  
  Send a single `POST` request to `localhost:8000/v1/query`. 
  Provide an authorization token in the header (see *Authentication* above) and the following body containing information about the target database, transaction type required, query, and optional options:
  ```json
  {
      "databaseName": "typedb",
      "transactionType": "read",
      "query": "match $p isa person, has name $n;",
      "queryOptions": { // optional
          "includeInstanceTypes": true
      },
      "commit": false // optional, does nothing in read queries
  }
  ``` 
  </details>
  
  
  #### Running big queries
  The first version of the HTTP endpoint does not support query answer streaming. It means that, unlike in gRPC, the query results will be fully consumed before an initial answer is received on the client side, and the whole list of concept rows or documents will be returned in a single response. 
  
  While this mechanism will be enhanced in the future, for safety purposes, please use a special query option `answerCountLimit` to limit the amount of produced results and avoid too long query execution. The default value of 1 million answers can be extended if you are ready for the consequences. 
  
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
  
  ### FAQ
  
  #### I want to run a large and/or complicated query that will take much time to execute. Does it require additional configuration?
  
  Make sure to provide sufficient transaction timeout and query answer count limit:
  ```
      "transactionOptions": {
          "transactionTimeoutMillis": 1000
      },
      "queryOptions": {
          "answerCountLimit": 100000
      }
  ```
  
  #### What happens if the authorization token expires while a large query is being executed?
  
  Active queries will not be interrupted, and the results will be returned (TODO: We can probably change it by passing an additional signal!). No new requests will be accepted until a new token is acquired.
  
  
- **Introduce branch provenance tracking**
  Introduce branch provenance tracking. This allows us to determine which branch of a disjunction a given answer originates from. Though meaningless in the logical semantics, this is helpful for other applications like visualisations.
  
  

## Bugs Fixed
- **Fix crashing race condition after write query**
  
  Resolve issue where a large write query (~100k keys) would sometimes cause the server to crash at the end.
  
  

## Code Refactors


## Other Improvements

- **Fix HTTP endpoint BDDs CI builds**
  Add a temporary data directory with write access for servers created in HTTP endpoint BDDs.
  
  
- **Simplify resolving role-types in writes**
  Refactors role-type resolution in insert, update & delete stages to be done in a single function.
  
  
    

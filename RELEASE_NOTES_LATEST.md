**Download from TypeDB Package Repository:**

[Distributions for 3.4.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.4.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.4.0-rc0```


## New Features
- **Introduce database export and import**
  Add database export and database import operations. Unlike in TypeDB 2.x, these operations are run through TypeDB **GRPC clients** such as the **Rust driver** or **TypeDB Console**, which solves a number of issues with networking and encryption, especially relevant for users of TypeDB Enterprise. With this, it becomes an official part of the TypeDB GPRC protocol. 
  Both operations are performed through the network, but the server and the client can be used on the same host.
  
  Each TypeDB database can be represented as two files:
  1. A text file with its TypeQL schema description: a complete `define` query for the whole schema.
  2. A binary file with its data.
  
  This format is an extension to the TypeDB 2.x's export format. See more details below for version compatibility information.
  
  ### Database export
  Database export allows a client to download database schema and data files from a TypeDB server for future import to the same or higher TypeDB version.
  
  The files are created on the client side. While the database data is being exported, parallel queries are allowed, but none of them will affect the exported data thanks to TypeDB's transactionality.
  However, the database will not be available for such operations as deletion.
  
  **Exported TypeDB 3.x databases cannot be imported into servers of older versions.**
  
  ### Database import
  Database import allows a client to upload previously exported database schema and data files to a TypeDB server. It is possible to assign any new name to the imported database.
  
  While the database data is being imported, it is not recommended to perform parallel operations against it.
  Interfering actions may lead to import errors or database corruption.
  
  **Import supports all exported TypeDB 2.x and TypeDB 3.x databases.** It can be used to migrate between TypeDB versions with breaking changes. Please visit [our docs](https://typedb.com/docs/manual/migration) for more information.
  
  

## Bugs Fixed
- **Remove "no open transaction" errors from HTTP transaction close endpoint**
  Remove the last possible and very rare case of the "no open transaction" error after calling `/transactions/:transaction-id/close` when the transaction is closed in parallel without answering the current request. 
  
  
- **Fix variable id allocator overflow crash**
  Return a representation error instead of a crash for queries overflowing the variable number limit. Fixes https://github.com/typedb/typedb/issues/7482.
  
  Rust driver's error example:
  ```
  called `Result::unwrap()` on an `Err` value: Server(
  [REP52] The query is too big and exceeds the limit of 65535 declared and anonymous variables. Please split it into parts and execute separately for better performance.
  Caused: [QEX7] Error in provided query.
  Near 16385:97
  -----
      $p15460 isa person, has name "XdelwJ-5275984974011080378", has id 13954001766678618073, has bio "Bio: 0R9641h09e3kGdTbHN4t";
      (friend: $p5460, friend: $p15460) isa friendship, has bio "Friendship#1568388083:Tn1VORIS-ZUwdem";
  --> $p5461 isa person, has name "k6HFQW-10247636131927110182", has id 14336185473951253852, has bio "Bio: OPPgVhYFdncrGBQ0xVv6";
                                                                                                      ^
      $p15461 isa person, has name "Fll5rq-125886840531597973", has id 10540830226431637304, has bio "Bio: KoJly7fs0HEPDiHiUBoz";
      (friend: $p5461, friend: $p15461) isa friendship, has bio "Friendship#3113679880:m6GqBBwt-144TBF";
  -----)
  ```
  
  
- **Fix forcing development mode in the config**
  Fix the compilation time forcing of development mode for local and snapshot builds


## Code Refactors

- **Commit statistics to WAL more frequently**
  
  Rebooting a large database can take a long time, due to having to synchronise statistics from the last durably written checkpoint in the WAL. This statistics record was written on a _percentage_ change of the statistics, which meant a lot of transactions could be committed before the statistics are checkpointed again. These all needed to be replayed to generate the newest statistics values on reboot. 
  
  Instead, each database now checkpoints the statistics either every 1k commits, or after am aggregate delta of 10k counts in the statistics, whichever comes first. This helps bound the amount of WAL entries replayed (& therefore memory required) for synchronising statistics on reboot. 
  
  Note: There is still a gap in this implementation, which occurs when there are 999 large commits happen but the aggregate delta is 0 (for example, by adding/deleting data continuously in large blocks) - this scenario would still require loading a large amount of the WAL into memory on replay.
  
  
- **Open-up server state component for extension**
  
  The `ServerState` component has been converted into a trait, with the actual implementation moved into a new component `LocalServerState`. This change is motivated by the need of allowing additional server state implementations with extended functionalities.
  
  
- **Update planner cost model for joins to reflect seeks being implemented**
  Updates the cost for a join between iterators to reflect seeks being introduced (#7367). The sum of the iterator sizes was accurate when we had to scan through the iterator. With seeks, we can skip impossible values. The number of seeks is on the order of the size of the smaller iterator.
  
  
  
- **Centralise server's state management**
  
  Server's state and state mutation has been centralised into a single struct, `ServerState`. This centralised state management structure makes it possible to reuse and also extend state management in TypeDB Cluster.
  
  

## Other Improvements

- **Send commit responses on commit requests instead of silently closing the stream**
  We utilize the already existing GRPC protocol's "commit" transaction response to explicitly mark that commit requests are successfully executed without errors. Previously, TypeDB clients only relied on either an error or a closure of the transaction stream, understanding the state of the transaction only based on its sent requests.

- **Update console artifact with the upgraded db import protocol**
  
  
- **Update README.md's style, contributions, and build instructions**
  Update the contributors list and minor styling moments to correspond to the current state of TypeDB.
  
  
    

**Download from TypeDB Package Repository:**

[Distributions for 3.4.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.4.1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.4.1```


## New Features
- **Enable RocksDB Prefix bloom filters**
  Enable [RocksDB bloom filters](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter), reducing avoidable disk-reads in large databases.


- **Refactor HTTP API output to include the structure of the pipeline**
  Adds the structure of the pipeline to the query structure returned by the HTTP API. Previously, we would just return a flat list of blocks without any information of how these blocks were connected (through stages and connectives such as or/not)


## Bugs Fixed
- **Handle trailing partial writes in WAL**

  We implement the ability to discard partially written trailing WAL records.


- **Improve intersection cost estimate, granular query plan cache eviction**

  We improve the cost estimate of an intersection by factoring in the work required to produce each element of the iterators involved. Previously the cost estimate assumed that said cost was limited to fetching a single key on disk, however when a post-filter is applied to a constraint iterator, we may have to fetch multiple keys to produce one that is accepted by all filters.

  Additionally, the query plan cache now takes into account the exact types used in the query to make eviction decisions on a plan-by-plan basis.


## Code Refactors
- **Rename config and CLI fields to align with the naming conventions**
  Perform the following renames in config files:
  * `server.http-enabled` -> `server.http.enabled`
  * `server.http-address` -> `server.http.address`
  * `server.encryption.certificate_key` -> `server.encryption.certificate-key`

  Perform the following renames in CLI arguments:
  * `logging.logdir` -> `logging.directory`


- **Simple renaming**

  We rename `VariableValue::Empty` to `None`, and rename `VariableDependency` to `VariableBindingMode`


- **Simplify PartialCostHash and beam search implementation**
  We simplify the hash used to detect redundant states during the plan-search, loosening the definition of equivalent plans for more aggressive pruning.


- **Reduce usages of assert! that don't need to be assert!**
  Allow `assert!` only in unrecoverable cases where we must crash the server.


- **Use refactored BDD layout**


## Other Improvements

- **Add HTTP authentication action diagnostics counts**
  Add diagnostics reporting for authentication actions performed through the HTTP endpoint.

  Previously, due to its excessive usage for every request and low value, we ignored this method. However, it's the most efficient metric to understand the usage of the HTTP endpoint of the server, so it is a valuable piece of usage metrics for us.



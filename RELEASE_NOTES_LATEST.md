**Download from TypeDB Package Repository:**

[Distributions for 3.14.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.14.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.14.2```


## New Features


## Bugs Fixed
- **Don't let transaction services excessively hold query manager refs**
  
  Fix a server crash when a database is deleted right after a transaction on it commits or closes. The delete could panic with "Cannot get exclusive ownership of inner of Arc<QueryCache>", stopping the server.
  
  
- **Let the HTTP endpoint accept clients without a client certificate, matching gRPC**
  
  Sync up the client certificate validation rules between gRPC and HTTP when `ca-certificate` is set. Now, the HTTP endpoint accepts clients without a client certificate, following what the gRPC endpoint already does. Client certificates that clients present are still verified against the CA.
  
  TypeDB drivers, Console, and Studio have no option to send a client certificate. When running a server like this:
  ```
  typedb server --server.encryption.enabled=true --server.encryption.certificate=server.pem \
      --server.encryption.certificate-key=server-key.pem --server.encryption.ca-certificate=ca.pem
  ```
  
  **Before:**
  - Console and drivers successfully connect over gRPC.
  - Studio, the HTTP drivers, and `curl --cacert ca.pem https://host:8000/v1/version` fail with `tlsv13 alert certificate required`.
  
  **After:** both protocols accept them.
  
  Setting `ca-certificate` does not make client certificates mandatory on either endpoint: it only rejects clients that present a certificate the CA did not sign. Most databases either use the same allowance pattern or explicitly expose "force client certificate verification" in the server's config. We support what already works in gRPC.
  
  
- **Fix concurrent read queries within a transaction over HTTP**
  
  We fix the implementation of multiple read queries being executed concurrently in HTTP services. Previously, we supported at most 1 read or write query a time. The new behaviour matches gRPC, where multiple read queries can operate concurrently exclusively against 1 write or schema query at a time.
  
  Other changes:
  - Writes, schema queries, rollback, and close interrupts running read queries
  - A commit interrupts all running read queries but does finish all queued write queries. 
  - A failed read or analyse only fails that request. Previously any read error closed the HTTP transaction.
  
  - A commit sent while a write was still running closed the transaction without committing, losing the write, while the write's client was told it had been applied (TSV23).
  - we fix a crash that occurs during a double 'interrupt' signal internally
  
  
- **Drop HTTP transaction service lock while handling request**
  
  TypeDB transactions are designed to allow concurrent read queries to be executed, while writes are exclusive (to prevent read/write issues on the mutable transaction buffer).
  
  This is correctly implemented in gRPC, but did not work in HTTP since we held the service locks while executing queries and other operations. We now drop the lock before handling the user's operation.
  
  

## Code Refactors
- **Add concurrent workers for import**
  
  Restructure the database importer around phases and worker threads so that imports can be parallelised. Items arrive on one channel from the import service, are collected into batches, and are handed to a `DatabaseImporterPhase` whose `ImportWorker` threads pull batches from a shared queue and write them in their own transactions.
  
  We also introduce locks and atomics where required to support multiple threads writing to the importer state.
  
  In this iteration, the entire import runs in 1 phase with 1 thread, and then we execute the remaining `Pending` items that were set aside as unfinished before directly in the `DatabaseImporter`.
  
  
- **Bound the query caches by query size, not entry count**
  
  Bound the query caches by the size of the queries they hold, not by their number of entries. Fixes #8034.
  
  The parse, translation and plan caches each held up to a fixed number of entries (200, 200 and 100), whatever their size. A parsed, translated or compiled pipeline grows with its query. So after a bulk load of large insert queries, the caches kept gigabytes of server heap (#8034).
  
  On 3.14.1 this memory is not released while the server idles. In #8034, 3.14.0-rc1 did release most of it.
  
  

## Other Improvements

    

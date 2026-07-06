**Download from TypeDB Package Repository:**

[Distributions for 3.12.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.12.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.12.0```


## New Features
- **Allow passing UUIDs for users and credentials on creation**
  
  Allow passing pre-created UUIDs for users and credentials when creating a new user. This is valuable in multiple situations: e.g., in a distributed environment, where the inner state of the server must be shared 1-to-1 with the other members of a cluster (before, the UUIDs were strictly generated locally, leading to different states).
  
  
- **Await transactions closing in transaction operator**
  
  Ensure the `TransactionOperator` closes all transactions requested to be closed before returning from the function. Before, it used to "fire and forget", which does not have enough guarantees for real use cases of these methods.
  
  
- **Add database-based methods to transaction operator**
  
  Extend `TransactionOperator` to allow checking and closing tracked transactions based on the database name. The existence check is mostly natural for the database name as it's impossible to delete the database in use (while the other params like `owner` and `type` do not have a similar responsibility).
  
  
- **Expose rocksdb memory configuration**
  
  We expose a pair of new memory tuning parameters: RocksDB cache size (`storage.rocksdb.cache-size`) and RocksDB write buffers limit (`storage.rocksdb.write-buffers-limit`). Tuning these gives a more predictable memory limit, and the limits are **shared across open databases**:
  
  ```
  storage:
      data-directory: "data"
      rocksdb:
          cache-size: 1gb
          write-buffers-limit: 512mb
  ```
  
  Configurable with integer `kb`, `mb`, or `gb` units.
  
  By default, we will set 1gb of cache and 512mb of write buffers. As these limits are approached:
  1) the cache will evict least recently used data blocks
  2) the write buffers will more eagerly flush to disk, even if not quite full.
  
  If the write buffers limit can't be maintained, then writes will be throttled.
  
  We expect that currently, with additional overheads, TypeDB will use around 2-2.5gb of RAM at the steady state.
  
  Note that the cache size limit is a _soft_ limit: TypeDB always caches RocksDB indexes and bloom filters, so eg. a 1GB cache limit maybe breached if more than 1GB of indexes and blooms exist.
  
  
- **@doc and @meta annotations**
  
  We implement `@doc("docstring")` and `@meta("key", "value")` schema annotations. These annotations can be attached to types and capabilities i.e. `owns`, `plays`, `relates`, and `sub`:
  
  ```php
  define
  entity person @doc("this represents an individual client") @meta("icon", ":silhouette.png"),
    owns name @doc("full name including title");
  ```
  ```php
  match
    $x isa client;
    let $x_icon = get_meta("icon", $t); # get each client with a UI display icon hint
  ```
  
  These annotations can also be attached to function definitions. They go at the end of the signature, before the colon introducing the body of the function:
  ```php
  define
  fun get_random_number() -> integer
      @doc("chosen by a fair dice roll"):
  match
    let $rand = 4;
  return first $rand;
  ```
  
  
- **Expose metrics extensions to extend monitoring reports**
  
  Add an additional API to the diagnostics manager to let the extensions of TypeDB insert their own metrics in the monitoring reports (both Prometheus and JSON). This way, libraries on top of `typedb` create can use the monitoring endpoint, combining typedb default metrics and any additional information they need on the page.
  
  ### Extension examples
  
  **Prometheus** (just appending additional lines to the end with no extra markers)
  ```
  # distribution: TypeDB
  # version: 3.10.0-alpha-1
  # os: Darwin arm64 15.7.3
  
  # HELP typedb_build_info Build and runtime identity of this TypeDB server.
  # TYPE typedb_build_info gauge
  typedb_build_info{version="3.10.0-alpha-1", distribution="TypeDB", os_name="Darwin", os_arch="arm64", os_version="15.7.3"} 1
  
  ....
  
  # HELP typedb_wal_fsync_duration_seconds WAL fsync latency.
  # TYPE typedb_wal_fsync_duration_seconds histogram
  
  # HELP raft_apply_total Number of committed entries applied to the state machine.
  # TYPE raft_apply_total counter
  raft_apply_total 0
  
  # HELP raft_messages_sent_total Outbound raft messages sent.
  # TYPE raft_messages_sent_total counter
  raft_messages_sent_total 0
  ```
  
  **json** (introduce an extra `extensions` key for every registered extension to avoid conflicts with already existing keys)
  ```json
  {
    "actions": [],
    "deploymentID": "test",
    "distribution": "TypeDB",
    "errors": [],
    "extensions": {
      "raft": {
        "counters": {
          "apply_total": 0,
          "messages_sent_total": 0,
        },
        }
      }
    },
    ...
  ```
  
  
- **Introduce 'given' stage for running the same query on multiple inputs**
  Introduces the TypeQL `given` stage which allows `rows" to be passed to a query as input. By running the same pipeline for multiple rows of input data (~~SIMD~~ SQMD?) in a single query, it avoids the network and query-compilation overhead that running multiple queries  (or just the query-compilation overhead in case of one large query). Since given rows are separate from the TypeQL query string, using to get input parameters into queries is safe from TypeQL-injection (c.f. working them into the TypeQL query string).
  
  **Examples:** 
  ```
  # Insert a new `person`s  with name =`$n` for each row given.
  given $n: string;
  insert $p isa person, has name == $n;
  ```
  
  ```
  # Get the name for each person `$p` in the given rows.
  given $p: person;
  match $p has name $n;
  ```
  
  For examples on how to construct the given rows in the GRPC drivers, Please check that driver's README on [github](https://github.com/typedb/typedb-driver)
  
  
- **Expose re-seeding database IID vertex generators**
  
  Expose a new method for extensions of TypeDB to re-seed local IID generators for databases. 
  
  While the sync up is performed on database loading, in distributed systems, the local generators can easily go out of sync: server 1 creates entities with IIDs 1, 2, and 3, then shares these writes with server 2; instead of sharing these generator updates with server 2 through network, we notify server 2 **when needed** to reload the local counters based on its storage to sync up its memory before performing new write operations. 
  
  How this works with different TypeDB setups:
  1. Single server -- no effect
  2. Multiple servers, single writer -- explicit and quite efficient in case the writer is selected relatively rare
  3. Multiple servers, multiple writes -- would require re-seeding for any committable transaction, but, since the whole design of the server does not support this, it is not considered.
  
  Additionally, fix the loading of system databases that prevented unrestricted access for recovered servers.
  
  

## Bugs Fixed
- **Instantly cleanup transaction operator's registry on transaction close**
  
  Fix a bug where the transaction operator's `has` methods returned false positive answers due to stale inner caches.
  
  
- **Fix protocol extension mismatch error handling**
  Fix how errors are handled when the driver protocol extension is ahead of the server - it previously crashed, now returns an error as expected. Sample message:
  
  
- **Fix git patch file for Windows**
  
  Fix the `git.patch` file used to enable Windows builds
  
  
- **Fix global log level overshadowing**
  
  Now, you can run the server binary with `RUST_LOG=debug`, switching all the packages to the global logging level. Previously, the provided global value was overridden by the constant set to `info`.
  
  This also allows other libs using `typedb` crate to conventionally use this env var, which was blocked before. 
  
  
- **Restore studio and console commands on bootup**
  
  After #7817 made advertise_address an Option that defaults to None, the "To connect:" Studio and Console block in print_serving_information was gated on advertise being Some and silently disappeared for the default configuration (no --server.address.advertise.* set). 
  
  We now fall back to the listen address when advertise is not configured, and substitute the unspecified bind 0.0.0.0 with the loopback 127.0.0.1 so the printed link/command is actually connectable from the same host.
  
  
- **Support redirects in any HTTP error**
  
  Expand the set of service errors that can return `421 Misdirected Request`, including transaction errors (e.g., on attempts to run write operations against a non-primary replica). This replicates the behavior of gRPC errors.
  
  Before, some `TransactionServiceError`s could contain the redirection errors, but they would be ignored because these categories of errors were not covered with the right conversion logic.
  
  

## Code Refactors
- **Optimize schema commit with in-memory preloaded snapshot**
  
  Optimize schema commit by preloading the entire schema storage range into a new in-memory data structure. This makes the thousands of validation and cache rebuild lookups up significantly faster compared to the previous approach of always going to RocksDB.
  
  As an example, a schema with 21k schema types being committed with a set of new types, went from 40s to ~9s for the complete schema revalidation and cache rebuilds.
  
  
- **Optimize iterator KMerge**
  
  We optimize the read path of our executors and iterators by letting the core KMerge iterator box iterators that are moved in and out of the internal heap frequently, and sometimes bypassing it entirely.
  
  We also introduce new executor microbenchmarks, which are extremely useful for optimizing the overhead of our iterators. See `//executor/benches/BUILD`, which also shows how to produce a flamegraph — confirmed working on Linux and on Mac 26.
  
  ```
    >> bazel run --compilation_mode=opt //executor/benches:bench_instructions -- --bench [ "has_reverse_unbound/multi_type" ] # bench specific test
  
    # To generate a flamegraph instead of timing, pass --profile-time <seconds>.
    # Criterion will run each matched bench under pprof's SIGPROF sampler and write
    # a flamegraph.svg per bench function.
    >> bazel run --compilation_mode=opt //executor/benches:bench_instructions -- --bench --profile-time 30 [ "has_reverse_unbound/multi_type" ] # bench specific test
  ```
  
- **Use Bazel `exclusive` tag for HTTP service tests instead of CLI --jobs=1**
  
  Replace the CI Bazel `--jobs=1` flag to ensure jobs aren't running in parallel and clashing for resources with the `exclusive` bazel tag.
  
  Note that the `exclusive` flag allows parallel builds but blocks parallel test phase execution. In addition, if we ever add it, it prevents remote execution.
  
  
- **Move the admin service from network connections to OS sockets and add password reset**
  
  Extend the local-only admin tool for password recovery functionality and additional protection. 
  
  If the admin user loses their password, there is currently no way to reset it without restoring from backup. Standard databases all ship an analogous local tool for those purposes, and we similarly reuse our admin tool: `user reset-password <username>`.
  
  With this, the admin endpoint is now reachable only by a process running as the typedb service account. Trust is anchored in OS-level access control rather than a credential mechanism layered on top of TCP (UDS file mode on Unix, DACL on Windows). No new network port, no new credential store.
  
  **Breaking changes to the admin tool configuration:**
  - The tool is now off by default. Provide the `--server.admin.enabled=true` parameter (or set it in the config file) to enable it. 
  - `--server.admin.port=<port>` is replaced by `--server.admin.socket-path=<endpoint>`, since the admin tool is no longer exposed to the network.
  
  To run the admin tool, you must provide the socket path of the server you're connecting to (previously, it was the server's admin service address):
  ```
  typedb admin --socket-path /var/run/typedb/admin.sock --command "user reset-password admin"
  ```
  
  **Important:** on Unix, install paths longer than ~108 chars will require an explicit `--server.admin.socket-path` outside of the directory because of the OS socket path length limitation (`SUN_LEN`). 
  
  The socket path is printed in the server's startup banner:
  
  ```
  Running TypeDB CE 3.11.5.
  Serving:
    gRPC:  0.0.0.0:1729
    HTTP:  0.0.0.0:8000
    Admin: /var/run/typedb/admin.sock (Unix socket)
  TLS: disabled
  ```
  
  `user reset-password` accepts the new password in three forms — for interactive use, scripted use, or piped automation:
  
  ```
  # Interactive prompt (TTY)
  typedb admin --socket-path /var/run/typedb/admin.sock --command "user reset-password admin"
  
  # Positional argument (whitespace-free passwords, exposed to the terminal history)
  typedb admin --socket-path /var/run/typedb/admin.sock --command "user reset-password admin new-password"
  
  # Piped via stdin (carries whitespace and special characters intact)
  echo 'pass with spaces!@#' | typedb admin --socket-path /var/run/typedb/admin.sock --command "user reset-password admin"
  ```
  
  Additionally, fixes a bug when the user endpoint returned `Success` for an attempt to update credentials for a non-existing user. Now, it correctly returns `Not found` instead, similarly to the delete method.
  
  

## Other Improvements
- **Fix 'function not found' for built-in functions in fetch**
  
  Built-in functions that are not explicitly enumerated in the grammar need to be resolved during translation. There was a leftover check that a function name not included in the typeql grammar must be in the index of user-defined functions, which of course does not hold for built-in functions. We modify that check to account for non-grammar built-ins.
  
  
- **Allow committing schema functions on abstract types**
  Allows schema functions which satisfy type-checking only through abstract types, even though the pattern can have no answers (as no concrete types satisfy the pattern). This is to allow writing modular tql files where functions are defined on abstract types. Other schema modules can concretise these types.
  Calling such a schema function in a query or preamble function will result in a type-inference error as preamble functions & queries are still required to be concretely satisfiable.
  
  
- **Add TypeDB Loader**
  
  - Add TypeDB Loader binary, accessible via `typedb loader`
  
  
- **Check fractional part within denominator in decimal parse**
  Checks that the fractional part is within the domain of representable values when parsing a `Decimal` from string.
  
  
- **Fix apt: remove empty server/data dir that collides with the data symlink**
  
  Remove the pre-created data directory from the bazel targets to avoid conflicts in apt.
  
  The Debian package built from current master installs a regular directory at `/opt/typedb/core/server/data` instead of the symlink to `/var/lib/typedb/core/data/` that the package is supposed to create. As a consequence, an apt-installed TypeDB writes its data files under `/opt/typedb/core/server/data` -- a directory the package owns and dpkg may rewrite on upgrade, instead of under the volume-managed `/var/lib/typedb/core/data/`, which is the path that:                                 
  - apt_empty_dirs provisions with mode 0777 at install time.                                                              
  - Operators expect to back up / mount / chown.                  
  - The deb's own `apt_symlinks` entry explicitly points at.
  
  Two entries collide inside the deb's data.tar.gz:
  - opt/typedb/core/server/data (symlink)
  - opt/typedb/core/server/data/ (directory)
  
  The server starts, the database files end up on disk (just in a different directory than designed), restarts preserve state. However, with the data going to the wrong directory, anyone running `tar czf backup.tgz /var/lib/typedb/`, mounting a separate disk at `/var/lib/typedb/core/data/`, or following typical sysadmin playbooks gets a silent no-op. Leading to losing data. Also, the same issue with `apt upgrade` and `apt purge`. 
  
  At [bd671fa29](https://github.com/typedb/typedb/commit/bd671fa29857f020554c8627bccf2d73043103ea) the empty server/data directory was supplied via the `assemble_targz(empty_directories=["server/data"], …)` parameter on each download archive (`assemble-server-*-{zip,targz}` and `assemble-all-*-{zip,targz}`). Those rules are unrelated to the apt build path, so the empty directory never reached the deb. apt only had the symlink. Working.
                                                                                                                           
  Commit [e4f16b8b4](https://github.com/typedb/typedb/commit/e4f16b8b40114eeb4ebf2f8b5bc053a60c823fdb) migrated the assembly rules from `assemble_targz` to `pkg_tar` and replaced the macro-local `empty_directories` parameter with a standalone `pkg_mkdirs(name =  "package-layout-server-dirs", dirs = ["server/data"])` rule, then included it inside the shared `package-typedb-server` filegroup. From that point on, every consumer of `package-typedb-server` inherits the empty directory -- including the apt-payload chain `package-typedb-server` -> `package-typedb-all` -> `package-typedb-all-targz`. The conflict with apt_symlinks is the consequence.  
  
  **This PR changes the way we distribute TypeDB packages**: they no longer contain the pre-created empty directory. However, they are created on the initial server boot up, and it seems even cleaner.
  
  
- **Add CARGO_BAZEL_GENERATOR_URL re-export to factory**
  Adds CARGO_BAZEL_GENERATOR_URL re-export to factory, to speed up CI by skipping rebuilding the `cargo-bazel` tool (5 minutes) for jobs where a rust target has to be compiled.
  
  
- **Restrict redirect errors from overriding grpc server error details**
  
  When receiving a `Redirect` response, the gRPC encoding of that error was used to destroy the typedb error code, silently overwriting it to `"REDIRECT"` instead of the canonical `"CSV9"` (cluster-side error), only to preserve it in the stack trace.
  
  With this fix, we preserve the original error code, combining it with the redirection metadata.
  
  
- **Add graceful SIGTERM handling for Unix systems**
  
  SIGTERM is now gracefully processed together with SIGINT, leading to a usual shutdown process with careful cleanup (exit 0, admin socket removed, in-flight requests drained). This includes commands like `kill <pid>`, `docker stop`, `systemctl stop`, and Kubernetes pod termination.
  
  SIGKILL is uncatchable by the OS and continues to hard-kill the server immediately. 
  
  *Note:* In case of a SIGKILL, the socket file is left in the filesystem, but is correctly overridden on the next server restart. 
  
  
- **Fix tar archive sizes and double http:// in Studio connection url**
  
  - Make `tar.gz` archives true archives to reduce their size from 90 MB to 27 MB (as it was before migration to new Bazel rules)
  - Fix double `http://http://` in the Studio connection link in case of the correct usage of `advertise-address`.
  
  
- **Set cargo bazel generator url for CircleCI**
  Sets cargo bazel generator url in CircleCI jobs, Using a prebuilt `cargo-bazel` tool and saving on compilation times
  
  
- **Fix docker entrypoint**
  
  Fix the recently introduced broken entrypoint for the Docker image: the binary the entrypoint points at did not exist inside the image.
  
  The regression was introduced in https://github.com/typedb/typedb/commit/e4f16b8b40114eeb4ebf2f8b5bc053a60c823fdb ("Fix CI pipelines after rules_python upgrade #7829"), which migrated the assembly rules from `assemble_targz`/`assemble_zip` to `pkg_tar`/`pkg_zip` with explicit `package_dir = "...-{version}"`. Inside the docker layer the binary lived at:
  
  ```
  /opt/typedb-server-linux-<arch>-<version>/typedb
  ```
  
  (e.g. `…-0.0.0/typedb` for snapshots, `…-3.11.6/typedb` for the next release), but the OCI `entrypoint` was still hardcoded to the pre-migration, un-suffixed path:
  
  ```
  /opt/typedb-server-linux-<arch>/typedb
  ```
  
  The downloadable `assemble-server-*` and `assemble-all-*` tarballs/zips are unchanged: they keep the `-{version}` suffix in their extracted directory names, which is what end users want when extracting a downloaded archive locally.
  
  Current release artifacts breakdown:
  ```
    Snapshot tar/zip contents (all 5 OS×arch matrices):
    typedb-all-<os>-<arch>-<v>/             
    ├── LICENSE                
    ├── typedb                       (typedb.bat on Windows)                                                                 
    ├── admin/typedb_admin_bin       (.exe on Windows)      
    ├── console/typedb_console_bin   (.exe on Windows)                                                                       
    └── server/                                                     
        ├── config.yml                          
        ├── typedb_server_bin        (.exe on Windows)                                                                       
        └── data/                    (empty)          
                                                                                                                           
    Docker layer contents (linux x86_64 + arm64):                   
    /opt/typedb-server-linux-<arch>/   ← matches OCI entrypoint thanks to this PR                                            
    ├── LICENSE                                                                  
    ├── typedb                                  
    ├── admin/typedb_admin_bin                                                                                               
    └── server/{config.yml, typedb_server_bin, data/}
    OCI metadata: Entrypoint = ["/opt/typedb-server-linux-<arch>/typedb", "server",                                          
    "--storage.data-directory=/var/lib/typedb/data"], WorkingDir = /opt/typedb-server-linux-<arch>, ExposedPorts = 1729/tcp, 
    8000/tcp, Volumes = /var/lib/typedb/data/.                                                                               
                                                                  
    APT package install layout:                                                                                              
    /opt/typedb/core/                      ← payload from package-typedb-all (includes console)                              
    ├── typedb, admin/, server/, console/, LICENSE                                                                           
    └── typedb.service                     (systemd unit)                                                                    
    Symlinks:                                                       
      /usr/local/bin/typedb              → /opt/typedb/core/typedb
      /opt/typedb/core/server/data       → /var/lib/typedb/core/data/                                                        
      /opt/typedb/core/server/logs       → /var/log/typedb/          
      /usr/lib/systemd/system/typedb.service → /opt/typedb/core/typedb.service  
  ```
  
  **Additionally**, rename `deploy-typedb-server` alias to `deploy-typedb-all`, because this target is related to `typedb-all-` artifacts.
  
  
- **Update all MODULE and Cargo references**
  
  Update the dependency references to all the latest commits
  
  
  
- **Fix CI pipelines after rules_python upgrade**
  Fixes our CircleCI deployment pipelines after the rules_python upgrade 
  
  
- **Skip building http service test targets on main ci build**
  Tag HTTP API tests as manual so we save time on CI.
  
  # Implementation details
  I couldn't get the server to reboot between runs when I merged the targets. I may need an after-test shutdown step.
  
- **Add bazel disk cache size and age limits**

- **Extend TypeDB prometheus endpoint with new metrics**
  
  Exposes per-database query/transaction latency histograms, lifecycle counters, in-flight load gauges, WAL fsync timing and bytes-written, and host process_*/resource gauges on the monitoring port in JSON and Prometheus formats. All metrics now update every 15 seconds.
  
  Built for benchmark and stability/soak testing, enabling scraping a running server and identifying memory, cpu, or disk bottlenecks.
  
  #### New metrics
  
    - `typedb_query_duration_seconds{database,kind=read|write|schema}` — per-kind p99 panels.
    - `typedb_transaction_duration_seconds{database,kind} + typedb_transaction_lifecycle_total{outcome=started|committed|rolled_back|closed}` — commit/abort/rollback ratios.
    - `typedb_queries_per_transaction` 
    - `typedb_load{database,client=grpc|http,kind}` 
    - `typedb_wal_fsync_duration_seconds, typedb_wal_bytes_written_total`
    - `process_* gauges + server_resources_count + typedb_build_info`
  
  We also print the metrics endpoint on bootup. If disabled, we print `disabled` - along with the same new pattern of display for our HTTP and Admin APIs.
  
  
- **Don't copy annotations of variables passing through the block**
  Avoids copying over the annotations which pass through a block without being referenced in it.
  
  
- **Skip rebuilding behaviour test binaries in main CI build**
  Skip rebuilding in main CI build to make it shorter
  
  
- **Propagate various CI optimisations**
  Move to local maven dependencies, Bump protobuf dependency use the precompiled compiler.
  
  
- **Remove stray hyphen in release notes**
  Remove stray hyphen in release notes
  
    

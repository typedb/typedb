Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.28.2-rc1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.28.2-rc1)

Server + Console: [Distributions for 2.28.2-rc1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.28.2-rc1)


## New Features


## Bugs Fixed


## Code Refactors


## Other Improvements
- **Update diagnostics service to version 1**
  We introduce an updated version of diagnostics sent from a `TypeDB` server. 
  
  1. `config.yml` gets a new field `deploymentID` for the `diagnostics` section. This field should be used for collecting the data from multiple servers of a single `TypeDB Cloud` deployment.
  2.  The updated diagnostics data contains more information about the server resources and details for each separate database. More details can be found in the examples below.
  3. For the `JSON` reporting, we calculated diffs between the current timestamp and the `sinceTimestamp` (the previous hour when the data had to be sent: it's updated even if we had errors sending the data for simplicity). For the `Prometheus` data, we send raw counts as `Prometheus` calculates diffs based on its queries and expects raw diagnostics from our side.
  4. For the `JSON` monitoring, we show only the incrementing counters from the start of the server just as for the Prometheus diagnostics data (also available through the monitoring page). This way, the content is different from the reporting data.
  5. The `schema` and `data` diagnostics about each specific database are sent only from the primary replica of a deployment at the moment of the diagnostics collection. The `connection` peak values diagnostics regarding a database are still reported by a non-primary replica if the database exists or there were established transactions within the last hour before the database had been deleted.
  6. If the statistics reporting is turned off in the config, we send a totally safe part of the diagnostics data once to notify the server about the moment when the diagnostics were turned off. No user data is shared in this snapshot (see examples below). This action is performed only if the server is up for 1 hour (to avoid our CI tests report data), and only if the server has not successfully sent such a snapshot after turning the statistics reporting off the last time. If there is an error in sending this snapshot, the server will try again after a restart (no extra logic here).
  
  Example diagnostics data for Prometheus (http://localhost:4104/metrics?format=prometheus):
  ```
  # distribution: TypeDB Core
  # version: 2.28.0
  # os: Mac OS X x86_64 14.2.1
  
  # TYPE server_resources_count gauge
  server_resources_count{kind="memoryUsedInBytes"} 68160245760
  server_resources_count{kind="memoryAvailableInBytes"} 559230976
  server_resources_count{kind="diskUsedInBytes"} 175619862528
  server_resources_count{kind="diskAvailableInBytes"} 1819598303232
  
  # TYPE typedb_schema_data_count gauge
  typedb_schema_data_count{database="212487319", kind="typeCount"} 74
  typedb_schema_data_count{database="212487319", kind="entityCount"} 2891
  typedb_schema_data_count{database="212487319", kind="relationCount"} 2466
  typedb_schema_data_count{database="212487319", kind="attributeCount"} 5832
  typedb_schema_data_count{database="212487319", kind="hasCount"} 13325
  typedb_schema_data_count{database="212487319", kind="roleCount"} 7984
  typedb_schema_data_count{database="212487319", kind="storageInBytes"} 2164793
  typedb_schema_data_count{database="212487319", kind="storageKeyCount"} 94028
  typedb_schema_data_count{database="3717486", kind="typeCount"} 5
  typedb_schema_data_count{database="3717486", kind="entityCount"} 0
  typedb_schema_data_count{database="3717486", kind="relationCount"} 0
  typedb_schema_data_count{database="3717486", kind="attributeCount"} 0
  typedb_schema_data_count{database="3717486", kind="hasCount"} 0
  typedb_schema_data_count{database="3717486", kind="roleCount"} 0
  typedb_schema_data_count{database="3717486", kind="storageInBytes"} 0
  typedb_schema_data_count{database="3717486", kind="storageKeyCount"} 0
  
  # TYPE typedb_attempted_requests_total counter
  typedb_attempted_requests_total{kind="CONNECTION_OPEN"} 4
  typedb_attempted_requests_total{kind="DATABASES_ALL"} 4
  typedb_attempted_requests_total{kind="DATABASES_GET"} 4
  typedb_attempted_requests_total{kind="SERVERS_ALL"} 4
  typedb_attempted_requests_total{database="212487319", kind="DATABASES_CONTAINS"} 2
  typedb_attempted_requests_total{database="212487319", kind="SESSION_OPEN"} 2
  typedb_attempted_requests_total{database="212487319", kind="TRANSACTION_EXECUTE"} 70
  typedb_attempted_requests_total{database="212487319", kind="SESSION_CLOSE"} 1
  typedb_attempted_requests_total{database="3717486", kind="DATABASES_CONTAINS"} 2
  typedb_attempted_requests_total{database="3717486", kind="SESSION_OPEN"} 2
  typedb_attempted_requests_total{database="3717486", kind="TRANSACTION_EXECUTE"} 54
  typedb_attempted_requests_total{database="3717486", kind="SESSION_CLOSE"} 1
  
  # TYPE typedb_successful_requests_total counter
  typedb_successful_requests_total{kind="CONNECTION_OPEN"} 4
  typedb_successful_requests_total{kind="DATABASES_ALL"} 4
  typedb_successful_requests_total{kind="DATABASES_GET"} 4
  typedb_successful_requests_total{kind="SERVERS_ALL"} 4
  typedb_successful_requests_total{kind="USER_TOKEN"} 8
  typedb_successful_requests_total{database="212487319", kind="DATABASES_CONTAINS"} 2
  typedb_successful_requests_total{database="212487319", kind="SESSION_OPEN"} 2
  typedb_successful_requests_total{database="212487319", kind="TRANSACTION_EXECUTE"} 67
  typedb_successful_requests_total{database="212487319", kind="SESSION_CLOSE"} 1
  typedb_successful_requests_total{database="3717486", kind="DATABASES_CONTAINS"} 2
  typedb_successful_requests_total{database="3717486", kind="SESSION_OPEN"} 2
  typedb_successful_requests_total{database="3717486", kind="TRANSACTION_EXECUTE"} 47
  typedb_successful_requests_total{database="3717486", kind="SESSION_CLOSE"} 1
  
  # TYPE typedb_error_total counter
  typedb_error_total{database="3717486", code="TYR03"} 5
  typedb_error_total{database="3717486", code="TXN08"} 2
  ```

  
- **Turn off statistics reporting in CI**
  We turn off the `--diagnostics.reporting.statistics` in our CI builds not to send non-real diagnostics data.
  
  In version 2.28 and earlier, this flag purely prevents `TypeDB` from sending any diagnostics data.
  In the upcoming version 2.28.1, this flag still allows `TypeDB` to send a single diagnostics snapshot with the information of when the diagnostics data has been turned off, but it happens only after the server runs for 1 hour, so we expect the CI builds not to reach this point and not to send any diagnostics data as well.

- **Update README.md**

    

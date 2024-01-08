Install & Run: https://typedb.com/docs/home/install


## New Features


## Bugs Fixed
- **Fix fetch sub-query aggregation null pointer**
  
  We fix a bug where a Fetch query with a Get-Aggregate subquery that returned an empty (ie. undefined) answer throw a null pointer exception.
  
  For example this used to error if 'Alice' doesn't have any salaries in the database, since a 'sum' is undefined for 0 entries.
  ```
  match
  $x isa person, has name $n; $n == "Alice";
  fetch
  $n as "name";
  total-salary: {
    match $x has salary $s;
    get $s; 
    sum $s;
  };
  ```
  
  We now return the following JSON structure
  ```
  [
    {
      "name": {"value": "Alice",  "type":  {"label": "name", "root": "attribute", "value_type": "string"}},
      "total-salary": null
    }
  ]
  ```
  
  

## Code Refactors


## Other Improvements
- **Implement server diagnostics**
  
  We implement completely anonymous diagnostics reporting in TypeDB Core using Sentry. This includes sending unexpected system errors and expected user errors to the backend reporting system. We also submit occasional system status updates for number of databases/database size (daily) and periodically profile a transaction. This instrumentation should help get real-world usage and error analysis to help guide the development and efforts in TypeDB.
  
  Error and diagnostic reporting can be completely disabled in the configuration file by setting:
  ```
  diagnostics:
    reporting:
      enable: false
  ```
  or booting the server with `--diagnostics.reporting.enable=false`.
  
  
  
    

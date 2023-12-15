Install & Run: https://typedb.com/docs/home/install

## API Changes

This version is backwards compatible at the network and data-storage layers.

The console connection command for TypeDB command has changed. Please use:
```
typedb console --core=<uri>
```
instead of
```
typedb console --server=<uri>
```

## New Features
- **Update dependencies: rules_rust 0.31, updated typedb-common runners**
  
  We update dependencies to make the changes transitively available to TypeDB Cloud.
  

## Bugs Fixed
- **Fetch query can infer fetched attributes**
  
  We fix #6952 , by allowing attribute fetching to trigger reasoning. For example;
  
  Given:
  ```
  define
  rule has-name: when { $x isa person; } then { $x has name "John"; };
  ```
  
  Then the query:
  ```
  match $x isa person;
  fetch $x: name;
  ```
  
  Can now retrieve `name "John"` as part of the fetch response.
  
  
  
- **Update assembly tests to use new console flags**
  
  Update assembly tests to use new `--core=<address>` flag in place of the old `--server=<address>`.
  
- **Null checks in TypeService**
  
  We expand null checking in transaction TypeService, which is used for handling Concept API type requests. This replaces the uninformative NPE with a Missing Concept error message.
  

## Code Refactors
- **Update Console artifact: Cloud connection rename**
  
  We update the console artifact to make it transitively available to the TypeDB Cloud distribution. The principal change in Console is the updated CLI flag to connect to a Cloud instance (`--cloud` rather than `--enterprise`).
  
- **Update entry point runner for better UX**
  
  We update to the updated entry point for the assembled TypeDB Core, which omits `console` from the usage help if it is not present to avoid confusion. See https://github.com/vaticle/typedb/issues/6942 for more details.
  
  The command to boot up the server (unchanged):
  ```
  typedb server --server.address=<address>
  ``` 
  The command to boot up the console:
  ```
  typedb console --core=<address>
  ```
  
  We also improve the UX of the windows version of the entry point. Console no longer opens in a new window, `--help` is printed inline as expected, and in the event of the server failure, the logs are displayed to the user. See https://github.com/vaticle/typedb-common/pull/158 for more details.
  
- **Fetch output format update: attribute type includes "value_type"**
  
  We update the serializiation schema of TypeQL Fetch query outputs: attribute type serialization now includes its `value_type`. This change makes the output symmetric between raw values and attributes:
  
  ```json
  {
  "raw_value": { "value": "...", "value_type": "string" },
  "attribute": { "value": "...", "type": { "label": "T", "value_type": "string", "root": "attribute" } }
  }
  ```
  
  

## Other Improvements
- **Update README.md**

- **Update README.md**

- **Update README.md**

- **Update README.md**

- **Update the readme file: fix TypeQL link**
  
  Update the readme to fix the TypeQL link.
  
  
- **Fix broken forum badge**
  
  The goal of this PR is to address a broken forum badge caused by a change in Discourse's statistics endpoint.
  
  
    

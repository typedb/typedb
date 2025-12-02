**Download from TypeDB Package Repository:**

[Distributions for 3.7.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.7.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.7.0```


## New Features
- **Implement abs and rounding functions for Decimal**
  
  We implement the following functions for `Decimal`:
  - `abs`,
  - `round`,
  - `ceil`,
  - `floor`.
  
  This completes the value type coverage for implemented intrinsic functions.
  
  
- **Revise HTTP & GRPC protocol compatibility**
  All HTTP messages will now silently ignore unused fields. This avoids breaking compatibility when an optional field is added to a client request payload. The server will simply ignore the field - This means the addition of any fields which may not be ignored must explicitly increment the API version. 
  GRPC messages will have an extension field going forward. Newer drivers (>3.5.0)  with older servers (<3.5.x) may face "forward compatibility" issues where a method in the driver does not exist on the server and returns an error. Newly added options may also be ignored by the older server.
  
- **Support try blocks in write stages**
  
  We implement `try {}` block handling in all write stages, viz. `insert`, `delete`, `put`, and `update`. Only top-level `try` blocks are currently allowed, with no nesting.
  
  

## Bugs Fixed
- **Consider include_query_structure option**
  We only include the pipeline structure in concept row responses if the include_query_structure flag in query options is set.
  
  
- **Allow named role to be fully specified label when encoding pipeline structure**
  Allows a named role to be fully specified label when encoding pipeline structure. This is needed to handle `match $r relates relation:role;`
  
  
- **Expression executor must copy over provenance**
  The expression executor was not copying over provenance from the input row. Fixes this.
  
  

## Code Refactors
- **Add in panic logging via the global panic_hook**
  
  We ensure that panics are written to the configured `tracing` log file, by intercepting the panic event and routing it via the logger. This code is borrowed from https://github.com/LukeMathWalker/tracing-panic ! 
  
  
  
- **Align HTTP analyze response with GRPC**
  Align HTTP analyze response with GRPC
  

## Other Improvements
- **Improve type-seeding failure error experience**
  We improve the error message returned when type-seeding fails before the iterative pruning step by including the constraint name in the message.
  We also make type-inference propagate labels across 'isa' constraints first, since these are likely to be the most informative. This makes for more intuitive error messages when the 'isa' constraint is the unsatisfiable one.
  
  
- **Prepare release 3.7.0-rc0**
  
  We update the release notes and bump the version to 3.7.0-rc0.
  
  
- **Fix debug assertion to be more specific**

- **Update CircleCI mac executors and xcode version**
  Update CircleCI mac executors to `m4pro.medium` and xcode version to `16.4.0` in view of upcoming deprecations.
  
  
- **Update PR template**

- **Add rustfmt test CI job**
  
  
- **Add analyze to GRPC**
  Add analyze to GRPC
  
  
- **Minimize variants of cucumber steps to reduce bloat**
  Cucumber codegen leads to significantly bloated rlibs. We remove unused step variant implementations (Given, When, Then) as low hanging fruit.
  
- **Add BDD testing for analyzing queries**
  Tests the query structure returned by the analyze endpoint (and used by studios' graph visualizer)
  
  
    

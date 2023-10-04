Install & Run: https://typedb.com/docs/typedb/2.x/installation


## New Features
- **Create separate distributions for each OS + Arch**

  We create 5 separate distributions of TypeDB, one per platform:

  1. `linux-x86_64`
  2. `linux-arm64`
  3. `mac-x86_64`
  4. `mac-arm64`
  5. `windows-x86_64`

  Please be aware that this means TypeDB distributions are no longer portable between Intel and Mac variants of the same system - eg. from an Intel mac to an ARM mac.


## Bugs Fixed
- **Handle transitivity in [Thing]TypeGetSubtypes requests**
  
  We previously ignored the `transitivity` field in the incoming `*TypeGetSubtypes()` requests, which led to `*TypeGetSubtypes(EXPLICIT)` returning _all_ transitive subtypes. This behaviour is now fixed.

- **Fix APT snapshot test, bazel install on Mac CI**

- **Downgrade CircleCI executors to ensure backwards compatibility**

- **Fix CircleCI assembly test executor types**

- **Update apt assembly to set empty directory permissions explicitly**
## Code Refactors

- **Replace 'client' terminology with 'driver' terminology**
  
  We replace the term 'cluster' with 'enterprise', to reflect the new consistent terminology used through Vaticle. We also replace 'client' with 'driver', where appropriate.

- **Deploy x86-64 and arm64 apt distributions**

  We deploy two apt distributions: `x86_64` (aka `amd64` in apt) and `arm64`. Apt automatically selects the correct architecture matching the current platform, so users just need to add the apt source and install the server.


- **Simplify Concept API & unify gRPC API with Cluster (#6761)**
  
  We simplify the `Concept` interface by unifying methods that were previously split into their general and `Explicit` variants. This allows us to simplify the gRPC protocol and deduplicate the implementation of the methods.
  
  In addition, we adjust the way TypeDB server responds to database and server manager requests to align it with TypeDB Cluster.
  

## Other Improvements
- **Reduce size of looping integration tests**

- **Include console artifact that includes both arm and intel architectures for mac and linux**

- **Delete sonarcloud code analysis job**

- **Replace occurrences of Cluster with Enterprise and Client with Driver, where appropriate**
    
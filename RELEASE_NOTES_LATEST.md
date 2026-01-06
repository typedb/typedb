**This is an alpha release for CLUSTERED TypeDB 3.x. Do not use this as a stable version of TypeDB.**
**Instead, reference a non-alpha release of the same major and minor versions.**

**Download from TypeDB Package Repository:**

[Distributions for 3.7.0-alpha-2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.7.0-alpha-2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.7.0-alpha-2```


## New Features

**Support TypeDB Cluster and user extensions**
TypeDB is rearchitected to support extensions like TypeDB Cluster. It is done through arced implementations of `ServerState`.
In order to boot up your own version of TypeDB, implement your own `impl ServerState` and replicate `main.rs` using your version
of the state.

All the features of TypeDB are preserved, but, since it's an alpha version, unexpected behaviour or bugs might be observed.

**Also note** that this server only supports GRPC Drivers and Console of the same version, and other versions of the clients
can lead to crashes. The HTTP endpoint is unchanged and safe to use similarly to the non-alpha version of TypeDB.

## Bugs Fixed

## Other Improvements
    

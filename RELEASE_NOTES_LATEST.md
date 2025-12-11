**Download from TypeDB Package Repository:**

[Distributions for 3.7.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.7.1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.7.1```


## New Features


## Bugs Fixed


## Code Refactors
- **Improve docker setup to use /var/lib/typedb/data for data dir**
  
  We improve the TypeDB Docker setup by using `/var/lib/typedb/data` for the architecture-agnostic data directory for TypeDB. This path is hardcoded into the built-in docker command for starting the command.
  
  This means, we now simplify the docker external volume mount to be:
  
  `docker volume create typedb-data` 
  and
  `docker create --name typedb -v typedb-data:/var/lib/typedb/data -p 1729:1729 -p 8000:8000 typedb/typedb:latest`
  
  Which works for either ARM or x86 builds.
  
  

## Other Improvements
- **Change cmd to entrypoint in docker setup**

    

**Download from TypeDB Package Repository:**

[Distributions for 3.0.3](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.3)

**Pull the Docker image:**

```docker pull typedb/typedb:3.0.3```


## New Features


## Bugs Fixed
- **Fix TypeDB reporting crashes when CA certificates are not found**
  We fix TypeDB reporting crashes when CA certificates are not found on the host. An additional Sentry (our crash reports endpoint) warning will be reported, but it no longer affects the server's availability. 
  
  

## Code Refactors


## Other Improvements
- **Make windows distribution directory structure consistent with other platforms**
  Makes the directory structure of the windows distribution consistent with other platforms.
  
  

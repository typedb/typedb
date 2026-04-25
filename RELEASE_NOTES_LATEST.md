**Download from TypeDB Package Repository:**

[Distributions for 3.10.3](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.10.3)

**Pull the Docker image:**

```docker pull typedb/typedb:3.10.3```


## New Features
- **Optional function returns are checked and print WARN log lines**
  Optional function returns are checked and any errors result in warnings being printed to the log. This is **temporary** to provide a path to non-breaking update and **will be escalated to query-compilation errors in a future release**.
  
  The following cases are checked:
  * A function which returns an optional variable is not declared as doing so. 
  * A variable being assigned an optional return value is not marked with `?`
  * A variable which was assigned an optional value is re-used in the same stage (subject to the same rules as regular optional variables).
  
  

## Bugs Fixed


## Code Refactors


## Other Improvements
- **Bazel java runtime is hermitic (remote) - used for notes creation**

- **Fix statistics counting**
  
  We fix one tracking error that means that statistics can still on rare occasions skip records that are being counted.
  
  
    

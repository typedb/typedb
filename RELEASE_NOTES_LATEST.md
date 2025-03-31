**Download from TypeDB Package Repository:**

[Distributions for 3.1.0-rc2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.1.0-rc2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.1.0-rc2```


## New Features


## Bugs Fixed
- **Extend Docker OS support and reduce TypeDB Docker image size**
  Update base Ubuntu images for Docker instead of generating intermediate layers through Bazel to potentially support more operating systems that expect additional configuration possibly missed in Bazel rules by default.
  
  This change is expected to allow more systems to run TypeDB Docker images, including Windows WSL, and significantly reduce the size of the final image.
  
  
- **Various fixes around variable usage**
  * Extend the check for empty type-annotations to include label vertices
  * Store function call arguments in a Vec, so they are ordered by argument index. 
  * Flag assignments to variables which were already bound in previous stages
  * Flag anonymous variables in unexpected pipeline stages instead of assuming named & unwrapping.
  
  
- **Fix incorrect set of input variables to nested patterns**
  
  During query plan lowering, we separate the current output variables (which represent the output variables of the plan being lowered) from variables available to the pattern being lowered, dubbed the row variables. 
  
  
- **Optimize unique constraints operation time checks for write queries**
  Modify the algorithm of attribute uniqueness validation at operation time, performed for each inserted `has` instance of ownerships with `@unique` and `@key` annotations. 
  These changes boost the performance of insert operations for constrained `has`es by up to 35 times, resulting in an almost indiscernible effect compared to the operations without uniqueness checks. 
  
  
- **Handle assignment to anonymous variables**
  
  We allow function assignment to discard values assigned to anonymous variables (such as in `let $_ = f($x);`) as there is no space allocated for those outputs. Previously the compilation would crash as it could not find the allocated slot for the value.
  
  
- **Fix write transactions left open and blocking other transactions after receiving errors**
  The gRPC service no longer leaves any of the transactions open after receiving transaction or query errors. This eliminates scenarios where schema transactions cannot open due to other hanging schema or write transactions and where read transactions can crash the server using incorrect operations (commits, rollbacks, ...). Additionally, it ensures the correctness of the load diagnostics data.
  
  

## Code Refactors
- **Improve readability of pattern executor**
  Improve readability of pattern executor
  
  
- **Simplify Batch by letting Vec do the work**
  Simplify Batch by letting Vec do the work
  
  
- **Make TypeDB logo configurable**
  No visible product changes
  
  

## Other Improvements
- **Add quotes to support spaces in windows launcher**
  Add quotes in typedb.bat to support spaces in the path to the TypeDB executable.
  
  
- **Update erorr message**

- **Sync behaviour tests transaction management based on query errors with the gRPC service**
  Behaviour tests now close the active transaction when a logical error is found in write and schema transactions, and leave transactions opened in case of a parsing error, which is how the real gRPC service acts.
  
  
- **Miscellaneous cleanup**
  
  - Remove unused imports
  - Apply automatic fixes (remove ummecessary references, clones, casts)
  - Autoformat
  
  
    

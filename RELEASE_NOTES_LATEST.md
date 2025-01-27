**Download from TypeDB Package Repository:**

[Distributions for 3.0.4](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.4)

**Pull the Docker image:**

```docker pull typedb/typedb:3.0.4```


## New Features


## Bugs Fixed
- **Add correct version for CLI --version flag. Substitute encryption-related config crashes by user errors.**
  Update the `--version` flag to return the correct version of the server when requested. 
  Prevent the server from crashing when an incorrect encryption configuration is supplied, stopping it gracefully and returning a defined error instead.
  
  

## Code Refactors
- **Add 'dec' suffix to display of decimal**
  Add 'dec' suffix to display of decimal
  
  

## Other Improvements
- **Add 'dec' suffix to display of decimal (#7326)**

- **Re-enable behaviour tests**
  Enables tests for various read & write behaviour.  
  

- **Cleanup todos**
  Replaces usages of the todo macro with either errors which will be returned, or custom macros if the line is unreachable. This reduces server crashes due to panics when execution reaches the todo.
  
  
    

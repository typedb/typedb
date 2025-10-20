**Download from TypeDB Package Repository:**

[Distributions for 3.5.5](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.5.5)

**Pull the Docker image:**

```docker pull typedb/typedb:3.5.5```


## New Features


## Bugs Fixed
- **Fix unreachable crashes in type seeder**
  For cases where the preconditions may not be satisfied by VariableCategory checks, we seed empty sets of types instead of having an "unreachable" panic.
  
  Fixes the crash in #7607 
  
  
- **Fix brew install**
  We now symlink `bin/typedb` to `libexec/typedb` instead of moving. This allows us to maintain the directory structure of the original distribution and allows the `typedb` script to correctly resolve the install directory.
  
  

## Code Refactors
- **Write type-check errors specify variable name**
  Write type check errors now provide the variable name instead of id.
  
  

## Other Improvements
- **Lower DB import/export logging to DEBUG**

    

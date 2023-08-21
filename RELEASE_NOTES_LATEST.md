Install & Run: https://typedb.com/docs/typedb/2.x/installation


## New Features


## Bugs Fixed
- **Shrink distributions by excluding unnecessary native jars**
  
  We discover that our distributions were larger than required, since they included every platform's native `or-tools` jars. We now manually exclude native or-tools from maven, and include them per-platform individually. This should reduce artifact size by 30-40 MB.
  
  
- **Add logic to allow unlimited archive age or sizes**
  
  The change in #6864 enabled configuring log archive retention policies by age or time, but would actually fail if trying to set unlimited time and size limits. The user can now set age limits or aggregate size limits by using the value `0` for either `archive-age-limit` or `archives-size-limit` configurations respectively.
  
  

## Code Refactors


## Other Improvements
  
    

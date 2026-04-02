**Download from TypeDB Package Repository:**

[Distributions for 3.8.3](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.8.3)

**Pull the Docker image:**

```docker pull typedb/typedb:3.8.3```


## New Features


## Bugs Fixed
- **Seed variables in comparison with attribute types for type-inference**
  Type-inference now seeds the variables involved in comparisons with attribute types. This avoids other kinds being involved in comparisons allows simpler downstream code, avoiding errors like #7697 
  
  

## Code Refactors


## Other Improvements
- **Increase posthog reporting interval to 2 hours**
  
  We increase the Posthog reporting interval to once every two hours.

  
- **Limit recovery read size when updating data statistics**
  
  We discard pre-loaded commit records when updating data statistics when the total size of data loaded from the WAL exceeds the limit (currently 1 GiB).
  
  
    

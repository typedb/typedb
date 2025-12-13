**Download from TypeDB Package Repository:**

[Distributions for 3.7.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.7.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.7.2```


## New Features
- **Implement min and max expressions**
  
  Implement min and max for expressions, usable for numerical types (`integer`, `double`, and `decimal`). Works for exactly 2 arguments:
  
  ```
  match 
  let $x = min(10, 12);
  let $y = max(10, 12);
  ```
  

## Bugs Fixed

- **Fix docker container build**

## Code Refactors


## Other Improvements
- **Update VERSION for release**


    

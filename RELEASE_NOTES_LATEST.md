**Download from TypeDB Package Repository:**

[Distributions for 3.10.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.10.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.10.2```


## New Features


## Bugs Fixed
- **Fix distinct stage stack overflow**
  
  Distinct stages relied on recursion instead of a loop, which could rapidly lead to stack overflows. We now use a simple loop instead.
- 
- **Fix statistics records reading**

  We fix a bug in statistics synchronization that led to out-of-sync statistics, which ultimately led to bad query planning.



## Code Refactors

## Other Improvements
  
    

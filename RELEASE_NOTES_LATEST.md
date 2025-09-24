**Download from TypeDB Package Repository:**

[Distributions for 3.5.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.5.1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.5.1```


## New Features


## Bugs Fixed
- **Only seek iterators in k-way merge when behind**
  
  Fix intermittent crashes with `Key behind the stored item in a Peekable iterator` when a roleplayer index constraint is part of a join.
  
## Code Refactors


## Other Improvements
- **Print non-typedb source errors**
  
  Print source of error when it's not a TypeDB error as well as part of the generated stack trace.
  

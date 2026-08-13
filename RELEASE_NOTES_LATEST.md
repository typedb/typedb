**Download from TypeDB Package Repository:**

[Distributions for 3.12.3](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.12.3)

**Pull the Docker image:**

```docker pull typedb/typedb:3.12.3```


## New Features


## Bugs Fixed
- **Fix database import blockers on inherited constraints**
  
  In specific situations, database import could be incorrectly rejected while relaxing or recovering the schema due to coincidental combinations of inherited constraints. We fix these cases completely.
  
  ### Independent sub attributes
  
  Independent sub attributes could lead to rejects on schema relaxation. At this stage, every attribute type must become independent so as not to lose data. However, double redeclarations of such annotations are prohibited, and an incorrectly working algorithm could produce a schema like `define attribute name @independent, value string; attribute surname @independent, sub name;`.
  
  ### Ownerships and roleplaying specializations
  
  A similar problem with `owns` and `plays` specializations using the same interface types (e.g., `define superperson owns name @card(0..); define person sub superperson, owns name @card(1..);`.
  
  While the algorithm correctly avoided conflicts in declared cardinalities, it could still have rare conflicts with other annotations, which, with the change of cardinality-based constraints, could lead to identical `owns`/`plays` declarations (which is, just like in the `attribute @independent` case, is prohibited). 
  
  

## Code Refactors
- **Add specific error message for 'Attribute' and 'Value' mismatch**
  
  Add a correction hint that help people fix Attribute and Value concept mismatches themselves.
  
  

## Other Improvements
- **Fix add_or_intersect change condition**
  Fixes the condition that determines whether add_or_intersect changed the type annotations of the vertex
  
  
- **Move unit tests from type-inference to match-inference**
  Straight copy paste + update imports.
  
    

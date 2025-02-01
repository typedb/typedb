**Download from TypeDB Package Repository:**

[Distributions for 3.0.5](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.5)

**Pull the Docker image:**

```docker pull typedb/typedb:3.0.5```


## New Features
- **Dramatically improve error messages with source query pointers**
  
  TypeDB now shows the source of an error in the context of the original query, wher it is possible. In general, we aim to show a detailed error message in context of the original query whenever the error arises in the compilation phase of the query.
  
  Example of the improved error format:
  ```
  [QEX2] Failed to execute define query.
  Near 4:29
  -----
          define
          attribute name value string;
  -->     entity person owns name @range(0..10);
                                  ^
        
  -----
  Caused by: 
        [DEX25] Defining annotation failed for type 'person'.
  Caused by: 
        [COW4] Concept write failed due to a schema validation error.
  Caused by: 
        [SVL34] Invalid arguments for range annotation '@range(0..10)' for value type 'Some(String)'.
  ```
  
  
- **Apply typedb error macro to top-level packages**
  
  TypeDB now reports extended error stack traces for all error types in the compiler and the intermediate representation builder, improving debuggability and ease-of-use.
  
  

## Bugs Fixed
- **Introduce role-player deduplication & Expand BDD coverage**
  Introduce role-player deduplication for when specified together in a single links constraint. i.e. `$r links (my-role: $p, my-role: $q);` will not use the same edge twice to satisfy each sub-constraint.
  Writing them as separate links constraint  `$r links (my-role: $p); $r links (my-role: $q);` will not de-duplicate.
  
  
- **Fix relates double specialization**
  Fix the behavior of `relates` specialization, featuring:
  * Unblocked double specialization:
  ```
  define 
    relation family-relation relates member @abstract;
    relation parentship sub family relation, relates parent as member, relates child as member; # Good!
  ```  
  * Fixed validations for multi-layered specializations:
  ```
  define
    relation family-relation relates member @abstract, relates pet @abstract;
    relation parentship sub family relation, relates parent as member;
    relation fathership sub parentship, relates father as member; # Bad!
    relation fathership sub parentship, relates fathers-dog as pet; # Good!
  ```
  * Better definition resolution and error messaging.
  * Change the inner terminology for generated `relates` for specialization: "root" and "non specializing" are substituted by "explicit", and "specializing" is substituted by "implicit".
  
  

## Code Refactors


## Other Improvements
- **Replace unwraps with panics for more detailed error messages**

- **Update automation.yml: return test_update**

- **Bugfix: direction of indexed relation instruction in lowering**
  
  We fix the logic in the lowering of query plans to executables that determines the direction of indexed relation instructions.
  
  
    

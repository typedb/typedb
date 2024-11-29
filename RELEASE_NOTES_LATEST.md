**Download from TypeDB Package Repository:**

[Distributions for 3.0.0-alpha-10](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.0-alpha-10)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-10```


## New Features
- **IID constraint**
  
  We add support for IID constraints in `match` clauses.
  
  Example:
  ```
  match $x iid 0x1E001A0000000012345678;
  ```
  
  

## Bugs Fixed
- **Fix greedy planner to sort on correct vars**
  
  Currently, the greedy planner picks a plan in form of an ordering of constraints and variables. An added constraint *produces* variables if it relates those variables and they haven't been added to the plan at an earlier stage. 
  
  The greedy planner picks constraints based on their "minimal" cost for a directional lookup, that may sort on either of one or more variables that the constraint relates. However, it does not record which variable the lookup should be sorted on. This means, as it stands, produced variables are added in random order after their constraint to the ordered plan.
  
  This PR aims to ensure that we sort on the variable for the appropriate direction, by adding them first.
  
  

## Code Refactors
- **Optimisations from typeql**
  
  Updates typeql for parsing optimisations.
  
  

## Other Improvements
- **Add BDD tests for functions**

  Add basic BDD tests for functions & implement validation.
  
  
- **Guarantee transaction causality**
  
  Under concurrent write workloads, causality can appear to be violated. For example:
  
  ```
  - Open tx1, tx2
  - start tx1 commit (validation begins)
  - start tx2 commit (validation begins and ends, no conflict with tx1!)
  - open tx3
  - end tx1 commit (validation finishes)
  ```
  When we open `tx3`, we end up with a snapshot that is actually from _before_ `tx1`, even though `tx2` has committed - because we don't know the status of `tx1` yet, and in our current simplified model, transaction are assigned a linear commit order decided at WAL write time, the read watermark remains before `tx1` until it finishes validating.
  
  In this scenario, a client that commits a transaction can actually end up opening the next transaction snapshot _before_ the last commit that successfully returned.
  
  After this change, when opening a transaction, TypeDB wait until the currently pending transactions all finish, guaranteeing we see the latest possible data version. The assumption is that 1) validation in general is a small amount of time and 2) is fully concurrent, so the wait time should be very small, and only occur under large concurrently committing transactions.
  
  
- **Frugal type seeder**
  
  Optimises parts of the type seeder to avoid unnecessary effort.
  
  
- **Improve Bytes ergonomics by allowing automatic dereference to byte slices**

    

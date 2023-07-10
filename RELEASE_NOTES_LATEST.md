Install & Run: http://docs.vaticle.com/docs/running-typedb/install-and-run


## New Features


## Bugs Fixed
- **Fix server hang-ups during shutdown**
  
  Under exceptional circumstances, such as when the server runs out of memory, the server could fail to shut down. We modify the server shutdown process and unexpected exception handler to shutdown in several stages, in the most severe case halting the JVM runtime immediately.
  
  
- **Made datetime attribute insert and match time-zone invariant**
  
  Enables the BDD tests to check timezone-invariance of inserting and reading datetime attributes.
  
  

## Code Refactors


## Other Improvements

- **Update SpeeDB to 2.4.1.5**

  This version of SpeeDB lowers the dependency version of GLIBC.

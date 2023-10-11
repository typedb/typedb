Install & Run: https://typedb.com/docs/typedb/2.x/installation


## New Features
- **Optimise storage lookup of IIDs**

  We optimise reasoner and traversal execution by never looking up IIDs in read transactions - they can trivially be converted into vertices in-memory. This is safe to do since we can verify all IIDs submitted into queries by the user are checked before execution, and all IIDs generated internally by the traversal engine or reasoner are always valid within the read transaction.

  This should yield a few percent performance improvement in common reasoner query execution.



## Bugs Fixed


## Code Refactors
- **Optional config values**

  We implement a new paradigm for configurations in TypeDB: optional primitive values. The outcome of this is that we have the following three top-level paradigms in our configuration file:

  1. All configuration file options are present in the configuration file
  2. 'Compound' (eg. nested blocks) of options that are optional are by convention protected by an `enable: true|false` option on the same level.
  3. For any optional primitive/leaf configurations, they may be left empty (or by YAML conventions set to `null` or `~`).

  Points 2 and 3 derive from point 1 in that they allow us to keep all configurations present, but introduce optionality for either entire blocks or leaf configurations.


## Other Improvements


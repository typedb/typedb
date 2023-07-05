Install & Run: http://docs.vaticle.com/docs/running-typedb/install-and-run

## Overview

This version adds support for the `ARM` architecture on Mac and Linux, replaces RocksDB with SpeedDB, improves the UX of export/import, and optimises the reasoner to re-use work more effectively.

## New Features
- **Add RPC logging for session and transaction creation** [[https://github.com/vaticle/typedb/pull/6836]]
  
  To monitor production instances of TypeDB and TypeDB enterprise more effectively, when using debug logging the server logs open client connections (each sessions and number of transactions each) at a rate of once per minute. When there are no connections, the server does not log to avoid noise in the idle state.
  
  To enable these logs, set a logger for 'com.vaticle.typedb.core.server` to DEBUG in the TypeDB configuration file.
  
  
- **Allow TypeDB migrator to operate over an empty database** [[https://github.com/vaticle/typedb/pull/6835]]
  
  We relax the Importer to operate not only over a non-existent database that is created on the fly, but also over a pre-existing and empty database. This is required because TypeDB Enterprise must create the database first, triggering a leader election, before doing the import against the primary replica.


- **Conjunction stream graph optimisation** [[PR#6826](https://github.com/vaticle/typedb/pull/6826)]

  Enable the reasoner to efficiently cache and reuse results of sub-conjunctions where possible. This can dramatically increase the performance of reasoning in some cases with many conjunctions.


- **Export and import schema and data file together** [[PR#6827](https://github.com/vaticle/typedb/pull/6827)]

  We improve the UX of importing and exporting data from TypeDB. Previously, we had to export the schema through the TypeDB Console independently, then create the database and define the schema through the Console at import time.

  Now both the import and export commands of TypeDB take two flags: `--schema` and `--data`, which for the export specify where to write the schema and data files to, and for the import specify where to read the schema and data files from.

  We implement validation to ensure that the database that is being imported into does not previously exist. Before, we allowed loading on top of an existing database, which could lead to unpredictable behavior.

  Closes [#6774](https://github.com/vaticle/typedb/issues/6774).


- **Native ARM builds for Mac and Linux** [[PR#6824](https://github.com/vaticle/typedb/pull/6824)]

  We upgrade our OR-Tools dependencies to the latest versions, which support both Linux and Mac ARM64 platforms. With these changes, we now have a TypeDB that can run on native ARM platforms such as M1/M2 Macs (`AArch64`) and Linux ARM processors (AWS Graviton, etc.).


- **Replace RocskDB with SpeeDB** [[PR#6818](https://github.com/vaticle/typedb/pull/6818)]

  Use SpeeDB instead of RocksDB as the underlying key-value store. It's a drop-in replacement.



## Bugs Fixed
- **Fix filtering of variables in update query** [[PR#6823](https://github.com/vaticle/typedb/pull/6823)]

  We fix the filtering of variables in an insert clause of update queries to allow inserting new entities.

  Fixes [#6549](https://github.com/vaticle/typedb/issues/6549).



## Code Refactors

- **Remove iterators where simple loops would suffice** [[PR#6828](https://github.com/vaticle/typedb/pull/6828)]

  We remove unnecessary usages of iterator objects to do for-each loops that were convenient but marginally slower. This is a minor optimisation, but also very easy to implement in several hot paths.



## Other Improvements
- **Fixed two minor typos in error messages**
  
  Fix two minor typos in error messages.

- **Optimise publisher registry's state management**

- **Fix Reasoner benchmark setup phase**

- **Simplify the structure of server/lib in assemblies & distributions** [[PR#6821](https://github.com/vaticle/typedb/pull/6821)]

  Simplify the structure of `server/lib` which currently contains 3 folders (`dev`, `prod`, and `common`) to directly contain all dependency jars.


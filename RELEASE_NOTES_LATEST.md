Install & Run: https://typedb.com/docs/home/install


## New Features
- **Allow directory to be created on bootup**
  
  We allow the data directory to be created on bootup, resolving https://github.com/vaticle/typedb/issues/6919
  
  
- **Allow parallelised concept and manager APIs**
  
  We implement https://github.com/vaticle/typedb/issues/6922, by allowing parallelised read queries for all protocol APIs that can be classified as read-only requests.
  
  

## Bugs Fixed


## Code Refactors
- **Assemble and deploy a single apt package**
  
  We no longer deploy `typedb-server` or `typedb-all` as a separate package, but rather a single package called `typedb` which contains server, console, and the binary. This approach now mirrors the 'brew' installation package.
  

## Other Improvements
- **Update readme - Add a link to the 25 queries page**
  
  Add a link to the TypeDB in 25 queries page to the readme file.
  
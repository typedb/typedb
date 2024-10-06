Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.29.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.29.0)

Server + Console: [Distributions for 2.29.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.29.0)


## New Features
- **Update TypeDB Console version**
  Update TypeDB Console to a version with an updated `password-update` command which allows specifying the password directly.


## Code Refactors
- **Cleaner logs when transaction close forcibly terminates reasoner**
  We update the reasoner to explicitly check whether the cause of termination was a transaction close or an exception, and only log in the latter case.
  

## Other Improvements
- **Update APT maintainer to TypeDB Community**
  
  The `maintainer` field of our APT package is now **TypeDB Community <community@typedb.com>**.

- **Fix large queries freezing server with new thread-pool for transaction requests**
  We introduce a separate thread-pool for servicing transaction requests. This avoids the case where multiple long running queries can occupy all the threads in the pool, and causing the server to be unable to process other requests. Critically, this allows session close calls to terminate any such transactions. 

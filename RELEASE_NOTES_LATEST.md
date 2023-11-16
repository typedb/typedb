Install & Run: https://typedb.com/docs/home/install


## New Features
- **Bundle TypeDB Console with improved error messages**

- **Ignore data directories that aren't TypeDB databases**

  TypeDB ignores directories in its configured 'data' directory that do not contain the subdirectories 'data' and 'schema'. In the past, the any directory was loaded as a TypeDB database, which could cause the server to crash on bootup.

  This should help with OS-created directories in some system (such as Lost+Found), or users' debugging directories.

## Bugs Fixed


## Code Refactors
- **Improve define error message when role type doesn't exist**


## Other Improvements
- **Remove sonarcloud dependencies**

- **Simplify Github templates**

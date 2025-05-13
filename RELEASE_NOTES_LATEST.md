**Download from TypeDB Package Repository:**

[Distributions for 3.3.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.3.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.3.0-rc0```


## New Features

- **Add yaml configuration file & logging to file**
  A yaml file is now required as the source of configuration settings.
  For documentation of the exposed settings, check the file at `server/config.yml`


## Bugs Fixed

- **Prune branch ids when pruning away unsatisfiable disjunction branches**
  This fixes a bug where the branch_ids go out of sync with the branches.

- **Update windows CI patch & guard against patching failure**
  Updates the windows CI patch to reflect a change in the target file & adds a check to fail if the patch was not applied properly

- **Apply the patch for Windows CI jobs only in prepare.bat**
  Fixes a bug where trying to apply the patch a second time fails and fails the script.


## Code Refactors


## Other Improvements
  
    

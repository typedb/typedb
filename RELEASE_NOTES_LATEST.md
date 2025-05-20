**Download from TypeDB Package Repository:**

[Distributions for 3.3.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.3.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.3.0```


## New Features

- **Add yaml configuration file & logging to file**
  A yaml file is now required as the source of configuration settings.
  For documentation of the exposed settings, check the file at `server/config.yml`

  

## Bugs Fixed
- **Add port 8000 to list of ports on docker image**
  Add port 8000 to the docker image, so the HTTP API is exposed. To be able to access it from the host machine, `-p 1729:1729 -p 8000:8000` must still be specified in the `docker create` or `docker run` commands.
  
  
- **Fix provenance updates from function calls**
  A function call wrongly copied the provenance of the returned function row. Instead, we copy the provenance of the row input to the call.
  
- **Fix variable category check for 'is' constraints**
  Fix the variable category check for 'is' constraints which would always fail, &  makes it recursive to catch more cases.
  
  
- **Add 'z' parsing for datetime-tz**
  
  We fix a bug where TypeDB server failed to recognise "Z" as a valid timezone specifier. According to ISO 8601 (https://en.wikipedia.org/wiki/ISO_8601#Coordinated_Universal_Time_(UTC)), this is valid timezone specifier equal to the 0 offset. The underlying time-zone library the server utilised did not accept it as a valid timezone. 
  
  Note that TypeQL already accepted 'z' as a valid timezone specifier in its grammar.
  
  
- **Fix variable visibility during translation**
  Fixes variable visibility tracking so that variables which are either: (1) disjunction local (2) deleted, or (3) reduced away; are not visible to subsequent stages. 



- **Apply the patch for Windows CI jobs only in prepare.bat**
  Fixes a bug where trying to apply the patch a second time fails and fails the script.

- **Update windows CI patch & guard against patching failure**
  Updates the windows CI patch to reflect a change in the target file & adds a check to fail if the patch was not applied properly


- **Prune branch ids when pruning away unsatisfiable disjunction branches**
  This fixes a bug where the branch_ids go out of sync with the branches.

- **Add label & kind constraints to query structure**
  Add label constraints (`match $t label person`) & kind constraints (`match entity $t;`) to query structure.

## Code Refactors
- **Add value type constraint to structure**
  Add value type constraint to structure
  
  
- **Update HTTP query response for coherence after adding query structure**
  Refactors `queryType` & `queryStructure` field in HTTP response; Refactors variable vertex in query structure output response
  
  
- **Rename involved_branches to involved_blocks**
  Rename `involved_branches` to `involved_blocks` in rows returned by the HTTP API 
  
- **Revise query structure output to be IR instead of edges**
  Revise query structure output to be IR instead of edges


## Other Improvements
- **Update behaviour with new datetime-tz test**

- **Overriding config file location via CLI is now relative to working directory**
  The config file override specified via '--config' is now relative to the working directory. It was previously relative to the server binary.
  Makes it easier to run typedb through bazel by copying the config file to the default location, avoiding the need for a CLI override.
  

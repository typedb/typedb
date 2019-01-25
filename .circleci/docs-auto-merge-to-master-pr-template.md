Docs release - merging everything from development to master (with the exception of client files)

## Why is this PR needed?
- This PR is submitted automatically by Grabl as a part of the release automation process.
- Before we can release the new version of Grakn + Documentation, changes made in the development(staging) branch of this repo need to be moved over to the master(production) branch.
- This merge excludes the changes made in the client files, as those changes need to be merged only when a new version of a Grakn Client needs to be released.
## What does the PR do?
- Creates the auto-merge-by-grabl off latest master
- Merges latest development to auto-merge-by-grabl excluding files at docs/03-client-api

# WHAT NEXT?

**Merge PR branch to master**
1. review and approve this PR
2. merge this PR
3. delete the PR branch

**Submit PR to merge master to development**
1. git pull graknlabs master
2. git co development
3. git pull graknlabs development
4. git checkout -b manual-merge-of-master-to-development
5. git merge master
6. git push fork-remote manual-merge-of-master-to-development
7. create the PR

**Merge new PR branch to development**
1. review and approve the new PR
2. merge the new PR

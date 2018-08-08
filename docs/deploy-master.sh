#!/bin/bash

set -e

function error {
    echo "The script failed with an error. Please remember to remove the temporary remote and branch which was created:"
    echo "git remote remove graknlabs-docs-temp-remote"
    echo "git branch -D graknlabs-docs-temp-branch"
}

trap error ERR

echo "Checking graknlabs/grakn:master int a temporary branch for deployment"
git stash
git remote add graknlabs-docs-temp-remote git@github.com:graknlabs/grakn.git
git fetch graknlabs-docs-temp-remote
git checkout -b graknlabs-docs-temp-branch graknlabs-docs-temp-remote/master
echo "Building _site"
rake build
echo "Staging new files"
# Forcefully add the _jekyll and _site folder before pushing to Heroku, this is necessary because both folders are ignored under the main .gitignore
git add --force -- _jekyll _site
git commit -m "Site update"
echo "Pushing to Heroku"
# we will just push the git tree for just the docs repo, by doing `git subtree split --prefix docs ...` from the root directory
cd ../
git push git@heroku.com:dev-grakn.git `git subtree split --prefix docs graknlabs-docs-temp-branch`:master --force
echo "Deleting temporary branch"
git checkout @{-1}
git remote remove graknlabs-docs-temp-remote
git branch -D graknlabs-docs-temp-branch
git stash pop

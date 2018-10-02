#!/bin/bash

set -e

function error {
    echo "The script failed with an error."
    echo "Please remember to remove the following remote and branch if you have them:"
    echo "git remote remove graknlabs-docs-temp-remote"
    echo "git branch -D graknlabs-docs-temp-branch"
}

trap error ERR

echo
echo "Checking out graknlabs/grakn:master into a temporary branch graknlabs-docs-temp-branch"
echo "..."
git stash
git remote add graknlabs-docs-temp-remote git@github.com:graknlabs/grakn.git
git fetch graknlabs-docs-temp-remote
git checkout -b graknlabs-docs-temp-branch graknlabs-docs-temp-remote/master

echo
echo "Building dev.grakn.ai website"
echo "..."
rake build

# Forcefully add the _jekyll and _site that are ignored for the main repository
git add --force -- _jekyll _site
git commit -m "Updating dev.grakn.ai"

echo
echo "Pushing website to git@heroku.com:grakn-web-dev.git"
echo "..."
# we will just push the git tree for just the docs repo, by doing `git subtree split --prefix docs ...` from the root directory
cd ../
git push git@heroku.com:grakn-web-dev.git `git subtree split --prefix docs graknlabs-docs-temp-branch`:master --force

echo
echo "Removing up temporary branch graknlabs-docs-temp-branch"
echo "..."
git checkout @{-1}
git remote remove graknlabs-docs-temp-remote
git branch -D graknlabs-docs-temp-branch
git stash pop

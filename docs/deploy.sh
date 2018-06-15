#!/usr/bin/env bash

set -e

function error {
    echo "The script failed with an error. Please remember to remove the temporary remote and branch which was created:"
    echo "git remote remove grakn-docs-origin"
    echo "git branch -D docs-deploy-temp"
}

trap error ERR

echo "Checking out temporary branch from stable"
git remote add grakn-docs-origin git@github.com:graknlabs/grakn.git
git fetch grakn-docs-origin
git checkout -b docs-deploy-temp grakn-docs-origin/stable
echo "Building _site"
rake build
echo "Staging new files"
git add -A
# Forcefully add the _jekyll and _site folder before pushing to Heroku, this is necessary because both folders are ignored under the main .gitignore
git add --force -- _jekyll _site
git commit -m "Site update"
echo "Pushing to Heroku"
cd ../
git push git@heroku.com:dev-grakn.git `git subtree split --prefix docs docs-deploy-temp`:master --force
echo "Deleting temporary branch"
git checkout @{-1}
git remote remove grakn-docs-origin
git branch -D docs-deploy-temp

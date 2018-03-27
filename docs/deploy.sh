#!/bin/bash
echo "Checking out temporary branch from stable"
git checkout -b docs-deploy-temp stable
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
git branch -D docs-deploy-temp
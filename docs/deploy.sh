#!/bin/bash

echo "Checking out temporary branch"
git checkout -b docs-deploy-temp
echo "Building _site"
rake build
echo "Staging new files"
git add -A 
git commit -m "Site update"
echo "Pushing to Heroku"
cd ../
git push https://git.heroku.com/docs-grakn.git `git subtree split --prefix docs docs-deploy-temp`:master --force
echo "Deleting temporary branch"
git checkout docs-redesign
git branch -D docs-deploy-temp
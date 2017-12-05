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
git subtree push --prefix docs https://git.heroku.com/docs-grakn.git master
echo "Deleting temporary branch"
git checkout docs-redesign
git branch -D docs-deploy-temp
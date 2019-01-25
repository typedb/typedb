import os
import re
import subprocess as sp

# TODO: consider not making grakn_master_branch configurable
git_username = "Grabl"
git_email = "grabl@grakn.ai"
grabl_credential = "grabl:"+os.environ['GRABL_TOKEN']

docs_url = "github.com/graknlabs/test-ci-docs.git"
docs_dev_branch = "development"
docs_clone_location = "test-ci-docs"

grakn_url = "github.com/graknlabs/test-ci-grakn.git"
grakn_master_branch = "refs/heads/master"

if __name__ == '__main__':
    try:
        print('This job will attempt to update the Grakn dependency in the WORKSPACE file in '+docs_url+' to the latest commit of '+grakn_url + '(' + grakn_master_branch + ' branch)')

        sp.check_output(["git", "config", "--global", "user.email", git_email], stderr=sp.STDOUT)
        sp.check_output(["git", "config", "--global", "user.name", git_username], stderr=sp.STDOUT)

        print('Cloning ' + docs_url + ' (' + docs_dev_branch + ' branch) to ' + docs_clone_location)
        sp.check_output(["git", "clone", "https://"+grabl_credential+"@"+docs_url, docs_clone_location], stderr=sp.STDOUT)
        sp.check_output(["git", "checkout", docs_dev_branch], cwd=docs_clone_location, stderr=sp.STDOUT)
        
        # update WORKSPACE with the new commit
        workspace_content = open(os.path.join(docs_clone_location, 'WORKSPACE'), 'r').readlines()
        new_commit = sp.check_output(["git", "ls-remote", "https://"+grabl_credential+"@"+grakn_url, grakn_master_branch], stderr=sp.STDOUT).split("\t")[0]    
        print('The Grakn dependency will be updated to to version "' + new_commit + '"')
        marker = 'grakn-dependency: do not remove this comment. this is used by the auto-update script'
        line_with_marker, _ = filter(lambda (index, line): line.find(marker) != -1, enumerate(workspace_content))[0]
        workspace_content[line_with_marker] = re.sub(r'[0-9a-f]{40}', new_commit, workspace_content[line_with_marker], 1)
        open(os.path.join(docs_clone_location, 'WORKSPACE'), 'w').write(''.join(workspace_content))

        # Commit and push the change
        # TODO: check if contents are changed using simple if checks rather than relying on the error message from git
        sp.check_output(["git", "add", "WORKSPACE"], cwd=docs_clone_location, stderr=sp.STDOUT)

        should_commit = sp.check_output(["git", "status"], cwd=docs_clone_location).find('nothing to commit, working tree clean') == -1
        if should_commit:
            print('Attempting to commit the updated version')
            sp.check_output(["git", "commit", "-m", "update grakn dependency to latest master"], cwd=docs_clone_location, stderr=sp.STDOUT)
            print('Pushing the change to ' + docs_url + ' (' + docs_dev_branch + ' branch)')
            sp.check_output(["git", "push", "https://"+grabl_credential+"@"+docs_url, docs_dev_branch], cwd=docs_clone_location, stderr=sp.STDOUT)
            print('Done!')
        else:
            print('WORKSPACE already contains the latest Grakn dependency. There is nothing to update.')
    except sp.CalledProcessError as e:
        print('An error occurred when running "' + str(e.cmd) + '". Process exited with code ' + str(e.returncode) + ' and message "' + e.output + '"')
        raise e
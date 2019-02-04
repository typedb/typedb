import os
import subprocess as sp

git_username = "Grabl"
git_email = "grabl@grakn.ai"
grabl_credential = "grabl:"+os.environ['GRABL_TOKEN']

# TODO: consider not making web_dev_master_branch configurable
web_dev_url = "github.com/graknlabs/test-ci-web-dev.git"
web_dev_master_branch = "master"
web_dev_clone_location = os.path.join("test-ci-web-dev")
docs_submodule_location = os.path.join(web_dev_clone_location, "test-ci-docs")

commit_msg = "update test-ci-docs submodule"

if __name__ == '__main__':
    try:
        print('This job will attempt to deploy the latest documentation to production')
        sp.check_output(["git", "config", "--global", "user.email", git_email])
        sp.check_output(["git", "config", "--global", "user.name", git_username])

        # --recursive clones web-dev as well as the docs submodule
        print('Cloning ' + web_dev_url + ' (' + web_dev_master_branch + ' branch)')
        sp.check_output(["git", "clone", "--recursive", "https://"+grabl_credential+"@"+web_dev_url, web_dev_clone_location], stderr=sp.STDOUT) # redirect stderr to silence the output from git
        
        print('Updating submodule at "' + docs_submodule_location + '"')
        sp.check_output(["git", "pull", "origin", web_dev_master_branch], cwd=docs_submodule_location) # TODO: replace origin
        sp.check_output(["git", "add", "."], cwd=web_dev_clone_location)
        
        should_commit = sp.check_output(["git", "status"], cwd=web_dev_clone_location).find('nothing to commit, working tree clean') == -1
        if should_commit:
            print('Deploying to production')
            sp.check_output(["git", "commit", "-m", commit_msg], cwd=web_dev_clone_location)
            sp.check_output(["git", "push", "https://"+grabl_credential+"@"+web_dev_url, web_dev_master_branch], cwd=web_dev_clone_location)
            print('Done!')
        else:
            print(web_dev_url + ' already contains the latest docs. There is nothing to deploy to production.')
    except sp.CalledProcessError as e:
        print('An error occurred when running "' + str(e.cmd) + '". Process exited with code ' + str(e.returncode) + ' and message "' + e.output + '"')
        raise e
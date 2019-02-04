import os
import subprocess as sp
import sys

git_username = "Grabl"
git_email = "grabl@grakn.ai"
grabl_credential = "grabl:"+os.environ['GRABL_TOKEN']

# TODO: consider not making grakn_master_branch configurable
docs_url = "github.com/graknlabs/test-ci-docs.git"
docs_dev_branch = "development"
docs_clone_location = "test-ci-docs"

# TODO: consider not making grakn_master_branch configurable
grakn_url = "github.com/graknlabs/test-ci-grakn.git"
grakn_master_branch = "refs/heads/master"

hub_download_url = "https://github.com/github/hub/releases/download/v2.7.0/hub-linux-amd64-2.7.0.tgz"
hub_download_location = "hub-linux-amd64-2.7.0"

pr_reviewer = 'sorsaffari'
pr_branch_name = "auto-merge-pr-by-grabl"
pr_msg_location = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'docs-auto-merge-to-master-pr-template.md')
commit_msg = "merge development branch with the exception of client files"

if __name__ == '__main__':
    try:
        # Assert if there is no existing branch with the same name as pr_branch_name
        check_branch = sp.check_output(["git", "ls-remote", "https://"+grabl_credential+"@"+docs_url, "refs/heads/" + pr_branch_name])
        if check_branch != '':
            print('ERROR: An existing ' + pr_branch_name + ' found in ' + docs_url + '.')
            sys.exit(1)

        print('The following git username / email will be used for committing: ' + git_username + ' / ' + git_email)
        sp.check_output(["git", "config", "--global", "user.email", git_email])
        sp.check_output(["git", "config", "--global", "user.name", git_username])

        print('Cloning ' + docs_url + ' to ' + docs_clone_location)
        sp.check_output(["git", "clone", "https://"+grabl_credential+"@"+docs_url, docs_clone_location], stderr=sp.STDOUT) # redirect stderr to silence the output from git
        sp.check_output(["git", "checkout", "master"], cwd=docs_clone_location)
        sp.check_output(["git", "checkout", "-b", pr_branch_name], cwd=docs_clone_location)

        # excluding certain files from merge > partial merge.
        # see: https://github.com/RWTH-EBC/AixLib/wiki/How-to:-Exclude-files-or-folder-from-merge
        sp.check_output(["git", "merge", "--no-commit", "--no-ff", "origin/development"], cwd=docs_clone_location)  # merge without commiting
        sp.check_output(["git", "reset", "HEAD", "--", "./03-client-api/"], cwd=docs_clone_location) # unstage the client files
        sp.check_output(["git", "stash", "save", "--keep-index", "--include-untracked"], cwd=docs_clone_location) # discard unstaged changes

        should_commit = sp.check_output(["git", "status"], cwd=docs_clone_location).find('nothing to commit, working tree clean') == -1
        if should_commit:
            sp.check_output(["git", "commit", "-m", commit_msg], cwd=docs_clone_location)
            sp.check_output(["git", "push", "https://"+grabl_credential+"@"+docs_url, pr_branch_name], cwd=docs_clone_location)
            
            # using `hub` to make a PR for the partial merge just made
            # see: https://hub.github.com/hub-pull-request.1.html
            sp.check_output(["wget", hub_download_url], cwd=docs_clone_location)
            sp.check_output(["tar", "-xvzf", hub_download_location+".tgz"], cwd=docs_clone_location)
            sp.check_output("GITHUB_TOKEN="+os.environ['GRABL_TOKEN']+" "+hub_download_location+"/bin/hub pull-request -F "+pr_msg_location+" -r " + pr_reviewer, shell=True, cwd=docs_clone_location)
            print("IMPORTANT - A PR was just created by Grabl on the docs repository. Make sure to follow the instructions in the PR's description before continuing this workflow.")
        else:
            print('There is nothing to update.')
    except sp.CalledProcessError as e:
        print('An error occurred when running "' + str(e.cmd) + '". Process exited with code ' + str(e.returncode) + ' and message "' + e.output + '"')
        raise e
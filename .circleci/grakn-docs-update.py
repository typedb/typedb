import os
import re
import subprocess as sp

repo_url = os.getenv('CIRCLE_REPOSITORY_URL')
building_upstream = 'graknlabs' in repo_url

if not building_upstream:
    print('Not building the upstream repo, no need to update the docs')
    exit(0)
elif building_upstream and 'GRABL_CREDENTIAL' not in os.environ:
    print('[ERROR]: Building the upstream repo requires having $GRABL_CREDENTIAL env variable')
    exit(1)

# TODO: consider not making grakn_master_branch configurable
git_username = "Grabl"
git_email = "grabl@grakn.ai"
grabl_credential = "grabl:"+os.environ['GRABL_CREDENTIAL']

docs_url = "github.com/graknlabs/docs.git"
docs_dev_branch = "development"
docs_clone_location = "docs"

grakn_url = "github.com/graknlabs/grakn.git"
grakn_master_branch = "refs/heads/master"

if __name__ == '__main__':
    try:
        print('** This job will make docs (development branch) depend on the latest grakn (master branch) **')
        new_commit = sp.check_output(["git", "ls-remote", "https://"+grabl_credential+"@"+grakn_url, grakn_master_branch], stderr=sp.STDOUT).split("\t")[0]    
        print('The latest commit in ' + grakn_url + '(' + grakn_master_branch + ' branch) is ' + new_commit)
        sp.check_output(["git", "config", "--global", "user.email", git_email], stderr=sp.STDOUT)
        sp.check_output(["git", "config", "--global", "user.name", git_username], stderr=sp.STDOUT)

        sp.check_output(["git", "clone", "https://"+grabl_credential+"@"+docs_url, docs_clone_location], stderr=sp.STDOUT)
        sp.check_output(["git", "checkout", docs_dev_branch], cwd=docs_clone_location, stderr=sp.STDOUT)
        print(docs_url + ' (' + docs_dev_branch +' branch) successfully cloned to ' + docs_clone_location)

        # update WORKSPACE with the new commit
        workspace_content = open(os.path.join(docs_clone_location, 'WORKSPACE'), 'r').readlines()
        marker = 'grakn-dependency: do not remove this comment. this is used by the auto-update script'
        line_with_marker, _ = filter(lambda (index, line): line.find(marker) != -1, enumerate(workspace_content))[0]
        workspace_content[line_with_marker] = re.sub(r'[0-9a-f]{40}', new_commit, workspace_content[line_with_marker], 1)
        open(os.path.join(docs_clone_location, 'WORKSPACE'), 'w').write(''.join(workspace_content))
        
        # Commit and push the change
        sp.check_output(["git", "add", "WORKSPACE"], cwd=docs_clone_location, stderr=sp.STDOUT)

        should_commit = sp.check_output(["git", "status"], cwd=docs_clone_location).find('nothing to commit, working tree clean') == -1
        if should_commit:
            sp.check_output(["git", "commit", "-m", "update grakn dependency to latest master"], cwd=docs_clone_location, stderr=sp.STDOUT)
            print('Pushing the change to ' + docs_url + ' (' + docs_dev_branch + ' branch)')
            sp.check_output(["git", "push", "https://"+grabl_credential+"@"+docs_url, docs_dev_branch], cwd=docs_clone_location, stderr=sp.STDOUT)
            print('The change has been pushed to ' + docs_url + ' (' + docs_dev_branch + ' branch)')
        else:
            print('docs already depends on ' + new_commit + '. There is nothing to update.')
    except sp.CalledProcessError as e:
        print('An error occurred when running "' + str(e.cmd) + '". Process exited with code ' + str(e.returncode) + ' and message "' + e.output + '"')
        raise e
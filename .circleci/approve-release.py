#!/usr/bin/env python

from __future__ import print_function

import os
import subprocess
import json


def check_output_discarding_stderr(*args, **kwargs):
    with open(os.devnull, 'w') as devnull:
        try:
            output = subprocess.check_output(*args, stderr=devnull, **kwargs)
            if type(output) == bytes:
                output = output.decode()
            return output
        except subprocess.CalledProcessError as e:
            print('An error occurred when running "' + str(e.cmd) + '". Process exited with code ' + str(
                e.returncode) + ' and message "' + e.output + '"')
            raise e

workflow_id = os.getenv('CIRCLE_WORKFLOW_ID')

GRABL_DATA = {
    'workflow-id': workflow_id,
    'repo': '{}/{}'.format(os.getenv('CIRCLE_PROJECT_USERNAME'), os.getenv('CIRCLE_PROJECT_REPONAME'))
}

GRABL_HOST = "http://grabl.herokuapp.com"
GRABL_URL_NEW = '{GRABL_HOST}/release/new'.format(GRABL_HOST=GRABL_HOST)
GRABL_URL_STATUS = '{GRABL_HOST}/release/{commit}/status'.format(GRABL_HOST=GRABL_HOST, commit=workflow_id)

print('Tests have been ran and everything is in a good, releasable state. It is possible to proceed with the release process')
_ = check_output_discarding_stderr([
    'curl', '-X', 'POST', '--data', json.dumps(GRABL_DATA), '-H', 'Content-Type: application/json', GRABL_URL_NEW
])

status = 'no-status'

while status == 'no-status':
    status = check_output_discarding_stderr(['curl', GRABL_URL_STATUS])

    if status == 'deploy':
        print("Deployment approved, creating the 'trigger-ci-release' branch "
              "in order to trigger the deployment process")
        subprocess.call(['git', 'branch', 'trigger-ci-release', 'HEAD'])
        subprocess.call(['git', 'push', 'origin', 'trigger-ci-release:trigger-ci-release'])
    elif status == 'do-not-deploy':
        print('Deployment has been manually rejected by an administrator')
        break
    elif status == 'timeout':
        print('Deployment rejected via timeout')
        break

#!/usr/bin/env python

import os
import subprocess as sp


def lprint(msg):
    # TODO: replace with proper logging
    from datetime import datetime
    print('[{}]: {}'.format(datetime.now().isoformat(), msg))


def gcloud_instances_create(instance):
    sp.check_call([
        'gcloud',
        'compute',
        'instances',
        'create',
        instance,
        '--image-family',
        'centos-7',
        '--image-project',
        'centos-cloud',
        '--machine-type',
        'n1-standard-4',
        '--zone=europe-west1-b',
        '--project=grakn-dev'
    ])


def gcloud_ssh(instance, command):
    sp.check_call([
        'gcloud',
        'compute',
        'ssh',
        instance,
        '--command=' + command,
        '--zone=europe-west1-b',
        '--project=grakn-dev'
    ])


def gcloud_scp(instance, local, remote):
    sp.check_call([
        'gcloud',
        'compute',
        'scp',
        local,
        instance + ':' + remote,
        '--zone=europe-west1-b',
        '--project=grakn-dev'
    ])


def gcloud_instances_delete(instance):
    sp.check_call([
        'gcloud',
        '--quiet',
        'compute',
        'instances',
        'delete',
        instance,
        '--delete-disks=all',
        '--zone=europe-west1-b'
    ])


def version_file_append_commit(version_file, commit_id):
    with open(version_file, 'r') as file:
        version_content = (file.read().strip() + commit_id).replace('-', '_')
    with open(version_file, 'w') as file:
        file.write(version_content)


# TODO: exit if CIRCLE_BUILD_NUM and $GCP_CREDENTIAL aren't present
credential = os.getenv('GCP_CREDENTIAL')
project = 'grakn-dev'
instance = 'circleci-' + os.getenv('CIRCLE_PROJECT_REPONAME') + '-' + os.getenv('CIRCLE_JOB') + '-' + os.getenv('CIRCLE_BUILD_NUM')

try:
    lprint('Configure the gcloud CLI')
    credential_file = '/tmp/gcp-credential.json'
    with open(credential_file, 'w') as f:
        f.write(credential)
    sp.check_call(['gcloud', 'auth', 'activate-service-account', '--key-file', credential_file])
    sp.check_call(['gcloud', 'config', 'set', 'project', project])
    sp.check_call(['ssh-keygen', '-t', 'rsa', '-b', '4096', '-N', '', '-f', os.path.expanduser('~/.ssh/google_compute_engine')])

    lprint('Creating a CentOS instance "' + instance + '"')
    gcloud_instances_create(instance)

    lprint('Installing dependencies')
    gcloud_ssh(instance, 'sudo yum install -y http://opensource.wandisco.com/centos/7/git/x86_64/wandisco-git-release-7-2.noarch.rpm')
    gcloud_ssh(instance, 'sudo yum update -y')
    gcloud_ssh(instance, 'sudo yum install -y sudo procps which gcc gcc-c++ python-devel unzip git java-1.8.0-openjdk-devel rpm-build yum-utils')
    gcloud_ssh(instance, 'curl -OL https://github.com/bazelbuild/bazel/releases/download/0.20.0/bazel-0.20.0-installer-linux-x86_64.sh')
    gcloud_ssh(instance, 'chmod +x bazel-0.20.0-installer-linux-x86_64.sh')
    gcloud_ssh(instance, 'sudo ./bazel-0.20.0-installer-linux-x86_64.sh')

    lprint('Copying grakn distribution from CircleCI job into "' + instance + '"')
    version_file_append_commit(version_file='VERSION', commit_id=os.getenv('CIRCLE_SHA1'))
    sp.check_call(['cat', 'VERSION'])
    sp.check_call(['zip', '-r', 'grakn.zip', '.'])
    gcloud_scp(instance, local='grakn.zip', remote='~')
    gcloud_ssh(instance, 'unzip grakn.zip')

    lprint('Installing RPM packages. Grakn will be available system-wide')
    gcloud_ssh(instance, 'sudo yum-config-manager --add-repo https://repo.grakn.ai/repository/meta/test-grakn-core.repo')
    gcloud_ssh(instance, 'sudo yum -y update')
    gcloud_ssh(instance, 'sudo yum -y install grakn-core-server')
    gcloud_ssh(instance, 'sudo yum -y install grakn-core-console')

    gcloud_ssh(instance, 'grakn server start')
    # gcloud_ssh(instance, 'bazel test //test-deployment:test-deployment --test_output=streamed --spawn_strategy=standalone --cache_test_results=no') # TODO: enable
    gcloud_ssh(instance, 'grakn server stop')
finally:
    lprint('Deleting the CentOS instance')
    gcloud_instances_delete(instance)
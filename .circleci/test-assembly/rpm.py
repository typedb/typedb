#!/usr/bin/env python

import os
import subprocess as sp


def lprint(msg):
    # TODO: replace with proper logging
    from datetime import datetime
    print('[{}]: {}'.format(datetime.now().isoformat(), msg))


def create(instance):
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


def ssh(instance, command):
    sp.check_call([
        'gcloud',
        'compute',
        'ssh',
        instance,
        '--command=' + command,
        '--zone=europe-west1-b',
        '--project=grakn-dev'
    ])


def scp(instance, local, remote):
    sp.check_call([
        'gcloud',
        'compute',
        'scp',
        local,
        instance + ':' + remote,
        '--zone=europe-west1-b',
        '--project=grakn-dev'
    ])


def delete(instance):
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
    create(instance)

    lprint('Installing dependencies')
    ssh(instance, 'sudo yum install -y http://opensource.wandisco.com/centos/7/git/x86_64/wandisco-git-release-7-2.noarch.rpm')
    ssh(instance, 'sudo yum update -y')
    ssh(instance, 'sudo yum install -y sudo procps which gcc gcc-c++ python-devel unzip git java-1.8.0-openjdk-devel rpm-build')
    ssh(instance, 'curl -OL https://github.com/bazelbuild/bazel/releases/download/0.20.0/bazel-0.20.0-installer-linux-x86_64.sh')
    ssh(instance, 'chmod +x bazel-0.20.0-installer-linux-x86_64.sh')
    ssh(instance, 'sudo ./bazel-0.20.0-installer-linux-x86_64.sh')

    lprint('Copying grakn distribution from CircleCI job into "' + instance + '"')
    sp.check_call(['zip', '-r', 'grakn.zip', '.'])
    scp(instance, local='grakn.zip', remote='~')
    ssh(instance, 'unzip grakn.zip')

    lprint('Building RPM packages')
    ssh(instance, 'bazel build //bin:assemble-rpm')
    ssh(instance, 'bazel build //server:assemble-rpm')
    # ssh(instance, 'bazel build //console:assemble-rpm') # TODO: enable

    lprint('Installing RPM packages. Grakn will be available system-wide')
    ssh(instance, 'sudo yum localinstall -y bazel-bin/bin/assemble-rpm-x86_64.rpm') # TODO: fix rpm name
    ssh(instance, 'sudo yum localinstall -y bazel-bin/server/assemble-rpm-x86_64.rpm') # TODO: fix rpm name
    # ssh(instance, 'sudo yum localinstall -y bazel-bin/console/assemble-rpm-x86_64.rpm') # TODO: a) fix rpm name b) enable

    ssh(instance, 'grakn server start')
    ssh(instance, 'bazel test //test-deployment:test-deployment --test_output=streamed --spawn_strategy=standalone --cache_test_results=no')
    ssh(instance, 'grakn server stop')
finally:
    lprint('Deleting the CentOS instance')
    # delete(instance)
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


def gcloud_scp_from_remote(instance, remote, local):
    sp.call([
        'gcloud',
        'compute',
        'scp',
        instance + ':' + remote,
        local,
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
    gcloud_ssh(instance, 'curl -OL https://raw.githubusercontent.com/graknlabs/build-tools/master/ci/install-bazel-linux.sh')
    gcloud_ssh(instance, 'sudo bash ./install-bazel-linux.sh')

    lprint('Copying grakn distribution from CircleCI job into "' + instance + '"')

    sp.check_call(['cat', 'VERSION'])
    sp.check_call(['zip', '-r', 'grakn.zip', '--exclude=*.git*', '.'])
    gcloud_scp(instance, local='grakn.zip', remote='~')
    gcloud_ssh(instance, 'mkdir /tmp/grakn && unzip grakn.zip -d /tmp/grakn')

    lprint('Installing RPM packages. Grakn will be available system-wide')
    gcloud_ssh(instance, 'sudo yum-config-manager --add-repo https://repo.grakn.ai/repository/meta/rpm-snapshot.repo')
    gcloud_ssh(instance, 'sudo yum-config-manager --add-repo https://repo.grakn.ai/repository/meta/rpm.repo')
    gcloud_ssh(instance, 'sudo yum -y update')
    gcloud_ssh(instance, 'sudo yum -y install grakn-core-all-$(cat /tmp/grakn/VERSION)')

    # TODO: how do we avoid having to chown?
    gcloud_ssh(instance, 'sudo chown -R $USER:$USER /opt/grakn')

    gcloud_ssh(instance, 'grakn server start')
    gcloud_ssh(instance, 'pushd /tmp/grakn && bazel test //test/common:grakn-application-test --test_output=streamed --spawn_strategy=standalone --cache_test_results=no && popd')
    gcloud_ssh(instance, 'grakn server stop')
finally:
    lprint('Copying logs from CentOS instance')
    gcloud_scp_from_remote(instance, remote='/opt/grakn/core/logs/grakn.log', local='./grakn.log')
    gcloud_scp_from_remote(instance, remote='/opt/grakn/core/logs/cassandra.log', local='./cassandra.log')
    lprint('Deleting the CentOS instance')
    gcloud_instances_delete(instance)

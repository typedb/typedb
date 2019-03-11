#!/usr/bin/env python

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


project = 'grakn-dev'
instance = 'circleci-test-assembly-rpm-test'

try:
    lprint('Set GCP project to "' + project + '"')
    sp.check_call(['gcloud', 'config', 'set', 'project', project])

    lprint('Creating a CentOS instance "' + instance + '"')
    create(instance)

    lprint('Installing dependencies')
    ssh('circleci-test-assembly-rpm-test', 'sudo yum install -y http://opensource.wandisco.com/centos/7/git/x86_64/wandisco-git-release-7-2.noarch.rpm')
    ssh('circleci-test-assembly-rpm-test', 'sudo yum update -y')
    ssh('circleci-test-assembly-rpm-test', 'sudo yum install -y sudo procps which gcc gcc-c++ python-devel unzip git java-1.8.0-openjdk-devel rpm-build')
    ssh('circleci-test-assembly-rpm-test', 'curl -OL https://github.com/bazelbuild/bazel/releases/download/0.20.0/bazel-0.20.0-installer-linux-x86_64.sh')
    ssh('circleci-test-assembly-rpm-test', 'chmod +x bazel-0.20.0-installer-linux-x86_64.sh')
    ssh('circleci-test-assembly-rpm-test', 'sudo ./bazel-0.20.0-installer-linux-x86_64.sh')

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
    ssh(instance, 'grakn server stop')
finally:
    lprint('Deleting the CentOS instance')
    delete(instance)
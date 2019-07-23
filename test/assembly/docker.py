#!/usr/bin/env python

import os
import subprocess as sp
import sys
import time

print('Building the image...')
sp.check_call(['bazel', 'run', '//:assemble-docker'])

print('Starting the image...')
sp.check_call(['docker', 'run', '-v', '{}:/grakn-core-all-linux/logs/'.format(os.getcwd()), '--name', 'grakn','-d', '--rm', '-ti', '-p', '127.0.0.1:48555:48555/tcp', 'bazel:assemble-docker'])
print('Docker status:')
sp.check_call(['docker', 'ps'])

sys.stdout.write('Waiting for the instance to be ready')
sys.stdout.flush()
timeout = 0 # TODO: add timeout
# TODO: fail if the docker image is dead
# upon a successful gRPC connection, the curl returns 0 in linux, and 8 in mac
while sp.call(['curl', '--output', '/dev/null', '--silent', '--head', '--fail', 'localhost:48555']) not in {0, 8}:
    sys.stdout.write('.')
    sys.stdout.flush()
    time.sleep(1)
print()

print('Running the test...')
sp.check_call(['bazel', 'test', '//test/common:grakn-application-test', '--test_output=streamed',
                          '--spawn_strategy=standalone', '--cache_test_results=no'])

print('Stopping the container...')
sp.check_call(['docker', 'kill', 'grakn'])
print('Done!')
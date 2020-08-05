#!/usr/bin/env python

import os
import socket
import subprocess as sp
import sys
import time

print('Building the image...')
sp.check_call(['bazel', 'run', '//:assemble-docker'])

print('Starting the image...')
sp.check_call(['docker', 'run', '-v', '{}:/grakn-core-all-linux/logs/'.format(os.getcwd()), '--name', 'grakn','-d', '--rm', '-ti', '-p', '127.0.0.1:48555:48555/tcp', 'bazel:assemble-docker'])
print('Docker status:')
sp.check_call(['docker', 'ps'])

print('Waiting 30s for the instance to be ready')
time.sleep(30)

print('Running the test...')
sp.check_call(['bazel', 'test', '//test/common:grakn-application-test', '--test_output=streamed',
                          '--spawn_strategy=standalone', '--cache_test_results=no'])

print('Stopping the container...')
sp.check_call(['docker', 'kill', 'grakn'])
print('Done!')

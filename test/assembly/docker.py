#!/usr/bin/env python

import os
import socket
import subprocess as sp
import sys
import time


def wait_for_port(port, host='localhost', timeout=30.0):
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.01)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError('Waited too long for the port {} on host {} to start accepting '
                                   'connections.'.format(port, host)) from ex


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
wait_for_port(48555)

print('Running the test...')
sp.check_call(['bazel', 'test', '//test/common:grakn-application-test', '--test_output=streamed',
                          '--spawn_strategy=standalone', '--cache_test_results=no'])

print('Stopping the container...')
sp.check_call(['docker', 'kill', 'grakn'])
print('Done!')

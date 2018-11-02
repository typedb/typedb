
init:
	pip3 install pipenv
	pipenv install --dev

test:
	grakn server start
	bazel test //client_python:integration_tests --force_python PY3 --python_path `which python3`

init:
	pip install pipenv
	pipenv install --dev

protobuf:
	pipenv run python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/*.proto

build: protobuf

test: build
	pipenv run nosetests

accept: build
	pipenv run behave features/grakn-spec/features --tags=-skip -D graknversion=$(GRAKNVERSION)
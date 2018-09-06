# Benchmarking

To get started, Grakn, Ignite, Elasticsearch and Zipkin need to be running.

## Apache Ignite
In the Apache Ignite installation directory, do:
```
bin/ignite.sh
```

## Elasticsearch
https://www.elastic.co/guide/en/elasticsearch/reference/6.3/zip-targz.html

In the elasticsearch installation directory, do:
```
./bin/elasticsearch
```

## Zipkin
https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md

In the zipkin installation directory, do:

```
STORAGE_TYPE=elasticsearch ES_HOSTS=http://localhost:9200 ES_INDEX="benchmarking" java -jar zipkin.jar
```
The above connects to a running Elasticsearch backend, which persists benchmarking data

To start without using Elasticsearch, do:
```
java -jar zipkin.jar
```

Access zipkin to see the spans recorded at: http://localhost:9411/zipkin/

Check elasticsearch is running by receiving a response from http://localhost:9200 in-browser

## Plotly Dashboard

We have a dashboard that reads ElasticSearch and creates graphs via Dash and Plotly.

To get up and running, you need pipenv and python >=3.6.0

1. `pipenv install` (installs package dependencies for the dashboard)
2. `pipenv shell` (you may need to modify the `python_version = "3.6"` if the python version you have is newer/not quite the same. Alternatively manage your python versions with `pyenv`.
3. in the `dashboard/` directory, run `python dashboard.py`
4. Navigate to `http:localhost:8050` to see the dashboard


## Executing Benchmarks and Generating Data

We define YAML config files to execute under `grakn-benchmark/src/main/resources/`

The entry point to rebuild, generate, and name executions of config files is `run.py`

Basic usage:
`run.py --config grakn-benchmark/src/main/resources/societal_config_1.yml --execution-name query-plan-mod-1 --keyspace benchmark --ignite-dir /Users/user/Documents/benchmarking-reqs/apache-ignite-fabric-2.6.0-bin/`

Notes:
* Naming the execution not required, default name is always prepended with current Date and `name` tag in the YAML file
* Keyspace is not required, defaults to `name` in the YAML file
* Because ignite is not currently embedded, we need the directory of the ignite bin folder to search for `jar` files which contain ignite drivers

Further examples:

Stop and re-unpack Grakn server, then run
`run.py --unpack-tar --config grakn-benchmark/src/main/resources/societal_config_1.yml`

Rebuild Grakn server, stop and remove the old one, untar, then run
`run.py --build-grakn --config grakn-benchmark/src/main/resources/societal_config_1.yml`

Rebuild Benchmarking and its dependencies and execute
`run.py --build-benchmark--alldeps --config grakn-benchmark/src/main/resources/societal_config_1.yml`






## Kibana
Kibana can be used for visualization, however we've since designed a dashboard using Plotly.

https://www.elastic.co/guide/en/kibana/current/setup.html

In the Kibana installation directory, do:

./bin/kibana

Access at:
http://localhost:5601

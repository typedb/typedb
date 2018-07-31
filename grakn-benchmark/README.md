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

## Kibana
https://www.elastic.co/guide/en/kibana/current/setup.html

In the Kibana installation directory, do:

./bin/kibana

Access at:
http://localhost:5601
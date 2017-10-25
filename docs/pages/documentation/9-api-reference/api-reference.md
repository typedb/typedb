---
title: Grakn API Reference
keywords: API Reference Documentation
tags: [graql, java, java-api]
sidebar: documentation_sidebar
permalink: /documentation/api-reference/api-reference.html
toc: false
---

Welcome to the API Reference landing page. From here, you can find the latest, and all previous, versions of our API reference.

## Javadoc Reference

[![Javadocs](https://javadoc.io/badge/ai.grakn/grakn.svg)](https://javadoc.io/doc/ai.grakn/grakn)

The latest version of our Javadocs API Reference documentation will always be posted at [(https://grakn.ai/javadocs.html).](https://grakn.ai/javadocs.html).   

You can use the drop down selector to choose previous versions of the API reference, should you want to refer back to them. 

{% include note.html content="Before Grakn v0.6.0, we used a different name (MindmapsDB); you can find API reference for versions of MindmapsDB from 0.1.0 - 0.5.0 [here](https://javadoc.io/doc/io.mindmaps/mindmaps-core/0.5.0)." %}

## API Basics

#### Request Headers

+ Accept

  Specifies the list of accepted data types to be returned by the server (i.e. that are accepted/understandable by the client). 

  Grakn uses two custom content types, `application/hal+json` and `application/graql+json` to format the results of graql queries. 

  `application/graql+json` <a name="graqlJsonContentType"></a> 

  ```json
  {
     "x":{
        "isa":"person",
        "id":"401584"
     }
  }
  ```

  `application/hal+json` <a name="halJsonContentType"></a> provides a [HAL](http://stateless.co/hal_specification.html) representation of a Graql query that easily allows for API navigation between knowledge base concepts.

  ```json
  [
     {
        "_baseType":"ENTITY",
        "_links":{
           "explore":[
              {
                 "href":"/dashboard/explore/401584?keyspace=grakn&offsetEmbedded=0"
              }
           ],
           "self":{
              "href":"/kb/concept/401584?keyspace=grakn&offsetEmbedded=0"
           }
        },
        "_embedded":{
           "isa":[
              {
                 "_direction":"OUT",
                 "_baseType":"ENTITY_TYPE",
                 "_name":"person",
                 "_links":{
                    "explore":[
                       {
                          "href":"/dashboard/explore/16632?keyspace=grakn&offsetEmbedded=0"
                       }
                    ],
                    "self":{
                       "href":"/kb/concept/16632?keyspace=grakn&offsetEmbedded=0"
                    }
                 },
                 "_id":"16632"
              }
           ]
        },
        "_type":"person",
        "_id":"401584"
     }
  ]
  ```

+ Content-type

  Specifies the content type of the information being supplied within the request. 

#### Response Headers

+ Content-type

+ Content-length

## `/kb/graql`

#### `POST /execute`

Executes a graql query on the server and builds a representation for each concept in the response.  

**Request Headers**
+ Accept
  + application/text
  + [application/hal+json](#halJsonContentType)
  + [application/graql+json](#graqlJsonContentType)

**Request Body**

Graql query that should be executed on the server.

**Query Parameters**

+ **keyspace** Keyspace where query should execute. Required.
+ **infer** Enables reasoner inference on the provided query. Required.
+ **materialise** Enables materialisation of reasoner results from the provided query. Required.

**Response Headers**

+ Content-type
  + application/json

**Response JSON Object**

+ **response** Query response from the knowledge base formatted as specified by the provided `Accept` header. 
+ **originalQuery** Query from the Request body 

**Status Codes**
+ 200 

+ 400  *Bad Request* The request or query was invalid.

  `{ exception : "label 'human' not found" }`

+ 401 *Unauthorized* Provided privileges are not authorized.

+ 406 *Invalid Content-type* Unsupported content type specified in request Accept header. 

  `{"exception":"Unsupported Content-Type [*/*] requested"}⏎`

+ 422 *Unprocessable Entity* Invalid knowledge base operation was attempted. 

**Request:**

```
curl -X POST -H "Accept:application/text" "http://localhost:4567/kb/graql/execute?keyspace=grakn&infer=false&materialise=false;" --data 'match $x isa person; limit 1;'
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 14:24:49 GMT
Content-Type: application/text
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

$x id "4240" isa person; 
```

#### `GET /` *Deprecated*

Execute **read-only** graql queries on a Grakn knowledge base. Read-only queries are defined as `match`, `compute` or `aggregate` queries that do not modify the knowledge base.

**Request Headers**

- Accept
  - application/text
  - [application/hal+json](#halJsonContentType)
  - [application/graql+json](#graqlJsonContentType)

**Query Parameters**

- **keyspace** Keyspace where query should execute. Required.
- **infer** Enables reasoner inference on the provided query. Required.
- **materialise** Enables materialisation of reasoner results from the provided query. Required.
- **query** Graql get query to be executed. Required.

**Response Headers**

- Content-type
  - application/text
  - [application/hal+json](#halJsonContentType)
  - [application/graql+json](#graqlJsonContentType)

**Response**

Query response from the knowledge base formatted as specified by the provided `Accept` header. 

**Status Codes**

- 200 

- 400  *Bad Request* The request or query was invalid.

  `{ exception : "label 'human' not found" }`

- 401 *Unauthorized* Provided privileges are not authorized.

- 405 *Method Not Allowed* Attempted to execute a non-get query.

  `{"exception":"Only \"read-only\" queries are allowed."}`

- 406 *Invalid Content-type* Unsupported content type specified in request Accept header. 

  `{"exception":"Unsupported Content-Type [*/*] requested"}`

- 422 *Unprocessable Entity* Invalid knowledge base operation was attempted. 

**Request:**

```
curl -H "Accept:application/graql+json" -X GET "http://localhost:4567/kb/graql?keyspace=grakn&infer=false&materialise=false&query=match%20%24x%20isa%20person;%20limit%201;"
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 14:25:20 GMT
Content-Type: application/graql+json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

[
   {
      "x":{
         "isa":"person",
         "id":"4240"
      }
   }
]
```

#### `POST /` *Deprecated*

Execute **insert** graql queries on a Grakn knowledge base. 

**Request Headers**

- Accept
  - application/text
  - [application/graql+json](#graqlJsonContentType)

**Request Body**

Graql insert query that should be executed on the server.

**Query Parameters**

- **keyspace** Keyspace where query should execute. Required.
- **infer** Enables reasoner inference on the provided query. Required.
- **materialise** Enables materialisation of reasoner results from the provided query. Required.

**Response Headers**

- Content-type
  - application/text
  - application/graql+json



**Response**

Query response from the knowledge base formatted as specified by the provided `Accept` header. 

**Status Codes**

- 200 

- 400  *Bad Request* The request or query was invalid.

  `{ exception : "label 'human' not found" }`

- 401 *Unauthorized* Provided privileges are not authorized.

- 405 *Method Not Allowed* Attempted to execute a non-insert query.

  `{"exception":"Only INSERT queries are allowed."}  `

- 406 *Invalid Content-type* Unsupported content type specified in request Accept header. 

  `{"exception":"Unsupported Content-Type [*/*] requested"}`

- 422 *Unprocessable Entity* Invalid knowledge base operation was attempted. 

**Request:**

```
curl -X POST -H "Accept:application/text" "http://localhost:4567/kb/graql?keyspace=grakn&infer=false&materialise=false;" --data 'insert $x isa person;' 
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 14:26:32 GMT
Content-Type: application/text
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

$x id "40964320" isa person;
```

#### `DELETE /` *Deprecated*

Execute **delete** graql queries on a Grakn knowledge base. 

**Request Headers**

- Accept
  - application/text

**Request Body**

Graql delete query that should be executed on the server.

**Query Parameters**

- **keyspace** Keyspace where query should execute. Required.
- **infer** Enables reasoner inference on the provided query. Required.
- **materialise** Enables materialisation of reasoner results from the provided query. Required.

**Response Headers**

- Content-type
  - application/text

**Status Codes**

- 200 

- 400  *Bad Request* The request or query was invalid.

  `{ exception : "label 'human' not found" }`

- 401 *Unauthorized* Provided privileges are not authorized.

- 405 *Method Not Allowed* Attempted to execute a non-delete query.

  `{"exception":"Only DELETE queries are allowed."}  `

- 422 *Unprocessable Entity* Invalid knowledge base operation was attempted. 

**Request:**

```
curl -X DELETE "http://localhost:4567/kb/graql?keyspace=grakn&infer=false&materialise=false;" --data 'match $x isa parentship; delete $x;'
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 14:27:04 GMT
Content-Type: application/text
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

{}
```

## `/kb` 

#### `GET /concept/{id}`

Returns the HAL representation of the specified concept. 

**Request Headers**

- Accept
  - [application/hal+json](#halJsonContentType)

**Query Parameters**

- **identifier** Identifier of the concept. Required.
- **keyspace** Keyspace where query should execute. Required.
- **offsetEmbedded** Offset to begin at for for embedded HAL concepts. Default 0. 
- **limitEmbedded** Limit on the number of embedded HAL concepts. Default -1 returns all embedded concepts.

**Response Headers**

- Content-type
  - [application/hal+json](#halJsonContentType)

**Status Codes**

- 200 

- 401 *Unauthorized* Provided privileges are not authorized. 

- 404 *Not Found* The requested concept was not found in the specified knowledge base. 

  `{"exception":"No concept with ID [1] exists in keyspace [grakn]"}`

- 406 *Invalid Content-type* Unsupported content type specified in request Accept header. 

  `{"exception":"Unsupported Content-Type [notvalid] requested"}`

**Request:**

```
curl -X DELETE "http://localhost:4567/kb/graql?keyspace=grakn&infer=false&materialise=false;" --data 'match $x isa parentship; delete $x;'
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 15:15:37 GMT
Content-Type: application/hal+json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

{
   "_baseType":"ENTITY",
   "_links":{
      "explore":[
         {
            "href":"/dashboard/explore/508000?keyspace=grakn&offsetEmbedded=0"
         }
      ],
      "self":{
         "href":"/kb/concept/508000?keyspace=grakn&offsetEmbedded=0"
      }
   },
   "_embedded":{
   		...
   },
   "_type":"person",
   "_id":"508000"
}
```

#### `GET /schema`

Returns all schema concepts in the specified knowledge base in a JSON object. 

**Request Headers**

- Accept
  - application/json

**Query Parameters**

- **keyspace** Keyspace where query should execute. Required. 

**Response Headers**

- Content-type
  - application/json

**Status Codes**

- 200 

- 401 *Unauthorized* Provided privileges are not authorized. 

- 406 *Invalid Content-type* Unsupported content type specified in request Accept header. 

  `{"exception":"Unsupported Content-Type [not-valid] requested"}`

**Request:**

```
curl -X GET "http://localhost:4567/kb/schema?keyspace=grakn"
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 14:27:04 GMT
Content-Type: application/json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

{
   "entities":[
      "event",
      "person",
      "entity",
      ...
   ],
   "roles":[
      "wife",
	  "spouse",
      "husband",
      "role",
      ...
   ],
   "resources":[
      "name",
      "surname",
      "resource",
   ],
   "relationships":[
      "marriage",
      "relationship",
      ...
   ]
}
```

## `/tasks`

#### `GET /`

Get tasks executing on the server or cluster that match all of the provided criteria. Returns a minimal amount of information: for more detailed task information see the single task GET request.  

**Query Parameters**

- **status**  Filters results to tasks of this status. Options are {`CREATED`, `RUNNING`, `COMPLETED`, `STOPPED`,`FAILED`}. Default returns tasks of all statuses.  
- **className** Filters results to tasks of this class.  Default returns all results. 
- **creator** Filters results by class that created the task. Default returns all results. 
- **limit** Limit the number of entries in the returned result. Default 0 returns all results. 
- **offset** Can be used in conjunction with limit for pagination. Default 0 returns all results.

**Response Headers**

- Content-type
  - application/json

**Status Codes**

- 200 
- 401 *Unauthorized* Provided privileges are not authorized. 
- 404 *Not Found* The specified task was not found on the server. 

**Request:**

```
curl -X GET "http://localhost:4567/tasks" 
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 15:45:03 GMT
Content-Type: application/json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

[
   {
      "creator":"ai.grakn.client.BatchExecutorClient",
      "runAt":1498664697751,
      "recurring":false,
      "className":"ai.grakn.engine.loader.MutatorTask",
      "id":"04e16459-ac56-4be5-b9d4-32ead920ce93",
      "status":"FAILED"
   }
]
```

#### `GET /{id}`

Get detailed information about the specified task that has been submitted to the server or cluster. 

*Note:* If the task has been submitted but has not yet been picked up by a server executor this endpoint will return a 404. 

**Query Parameters**

- **id** Identifier of the task to retreive, previously supplied by the server. Required.

**Response Headers**

- Content-type
  - application/json

**Status Codes**

- 200 
- 401 *Unauthorized* Provided privileges are not authorized. 
- 404 *Not Found* The specified task was not found on the server. 

**Request:**

```
curl -X GET "http://localhost:4567/tasks/04e16459-ac56-4be5-b9d4-32ead920ce93" 
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 15:57:01 GMT
Content-Type: application/json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

{
   "exception":"ai.grakn.exception.GraqlSyntaxException",
   "creator":"ai.grakn.client.BatchExecutorClient",
   "runAt":1498664697751,
   "recurring":false,
   "className":"ai.grakn.engine.loader.MutatorTask",
   "interval":null,
   "id":"04e16459-ac56-4be5-b9d4-32ead920ce93",
   "stackTrace":"ai.grakn.exception.GraqlSyntaxException: syntax error at line 51: 
   $98424 has date \"1884-10-21\" isa wedding has identifier \"w04\" has confidence \"high\";
              ^
              no viable alternative at input 'has date' syntax error at line 53: 
      at ai.grakn.exception.GraqlSyntaxException.parsingError(GraqlSyntaxException.java:47)
      at ai.grakn.graql.internal.parser.QueryParser.parseQueryFragment(QueryParser.java:242)
      at ai.grakn.graql.internal.parser.QueryParser.parseQueryFragment(QueryParser.java:225)
      at ai.grakn.graql.internal.parser.QueryParser.parseQuery(QueryParser.java:131)
      at ai.grakn.graql.internal.query.QueryBuilderImpl.parse(QueryBuilderImpl.java:166)
      at java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:193)
      at java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:193)
      at java.util.ArrayList$ArrayListSpliterator.forEachRemaining(ArrayList.java:1374)
      at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:481)
      at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471)
      at java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:708)
      at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
      at java.util.stream.ReferencePipeline.collect(ReferencePipeline.java:499)
      at ai.grakn.engine.loader.MutatorTask.getInserts(MutatorTask.java:108)
      at ai.grakn.engine.loader.MutatorTask.start(MutatorTask.java:56)
      at ai.grakn.engine.tasks.manager.StandaloneTaskManager.lambda$executeTask$2(StandaloneTaskManager.java:201)
      at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
      at java.util.concurrent.FutureTask.run(FutureTask.java:266)
      at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
      at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
      at java.lang.Thread.run(Thread.java:745)
      ",
   "status":"FAILED",
   "engineID":"engine1"
}
```

#### `PUT /{id}/stop`

Attempt to stop a given task from executing. Note that this does not guarantee the task will be stopped. Subsequent calls to get status are necessary to ensure the final status of the task. 

**Query Parameters**

- **id** Identifier of the task to retrieve, previously supplied by the server. Required.

**Response Headers**

- Content-type
  - application/json

**Status Codes**

- 200 
- 401 *Unauthorized* Provided privileges are not authorized. 
- 404 *Not Found* The specified task was not found on the server. 

**Request:**

```
curl -X GET "http://localhost:4567/tasks/04e16459-ac56-4be5-b9d4-32ead920ce93" 
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 16:12:05 GMT
Content-Type: application/json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

{}
```

#### `POST /tasks`

Send tasks to be executed on the server or cluster. 

**Request Body**

The request body is a JSON object containing task parameters and configurations to be executed on the server or cluster. 

This JSON object will contain a single array of object, `tasks`, each containing the following parameters:

+ **className** Class of the task to run. Required. 
+ **creator** Class creating the task. Required. 
+ **runAt** Instant at which the task should run. This can be at any time: if the time is before the current moment, the task will be run as soon as it  reaches the Grakn engine executors. Required. 
+ **interval** Integer representing how often to run the task. Default is -1, the task will only be executed once. 
+ **priority** One of {`LOW`, `HIGH`} describing the priority of this task compared to others. Default is `LOW`.
+ **configuration** Data on which to execute the task. Default is an empty JSON object. 

One of the most common uses of the tasks API is to submit batch loading tasks to be executed on a cluster. The `configuration` object will change depending on the type of task submitted. An example of the configuration for a `MutatorTask`:

```
{
   "tasks":[
      {
         "creator":"ai.grakn.client.BatchExecutorClient",
         "runAt":"1498667812828",
         "configuration":{
            "keyspace":"grakn",
            "mutations":[
               "insert $p0 isa person has identifier \"Elizabeth Niesz\";
            ],
            "batchNumber":0
         },
         "limit":"10000",
         "className":"ai.grakn.engine.loader.MutatorTask"
      }
   ]
}
```

**Status Codes**

- 200 

- 202 *Accepted*

- 400 *Bad Request* Missing or malformed request body or task configuration. 

  `{"exception":"Missing mandatory parameter in body [tasks]"}`

- 401 *Unauthorized* Provided privileges are not authorized. 

**Request:**

```
curl -X POST "http://localhost:4567/tasks" --data '{"tasks":[{"creator":"ai.grakn.client.BatchExecutorClient","runAt":"1498667812828","configuration":{"keyspace":"grakn","mutations":["insert $p0 isa person has identifier \"Elizabeth Niesz\";"],"batchNumber":0},"limit":"10000","className":"ai.grakn.engine.loader.MutatorTask"}]}' -g 
```

**Response:**

```
HTTP/1.1 200 OK
Date: Wed, 28 Jun 2017 17:05:37 GMT
Content-Type: application/json
Transfer-Encoding: chunked
Server: Jetty(9.3.6.v20151106)

[
   {
      "code":200,
      "index":0,
      "id":"92239608-6075-4035-a781-93847929a23f"
   }
]
```

## Feedback
We hope you find our documentation helpful, but if you need more information, please don't hesitate to contact us using our [discussion forums](http://discuss.grakn.ai) or through our main [website](http://www.grakn.ai).

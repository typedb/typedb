---
title: Grakn API Reference
keywords: API Reference Documentation
tags: [graql, java, java-api]
sidebar: documentation_sidebar
permalink: /docs/api-references/rest-api
toc: false
---

Welcome to the API Reference landing page. From here, you can find the latest, and all previous, versions of our API reference.

## Javadoc Reference

[![Javadocs](https://javadoc.io/badge/ai.grakn/grakn.svg)](https://javadoc.io/doc/ai.grakn/grakn)

The latest version of our Javadocs API Reference documentation will always be posted at [(http://javadoc.io/doc/ai.grakn/grakn).](http://javadoc.io/doc/ai.grakn/grakn).   

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
curl -X POST -H "Accept:application/text" "http://localhost:4567/kb/graql/execute?keyspace=grakn&infer=false" --data 'match $x isa person; limit 1;'
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
curl -H "Accept:application/graql+json" -X GET "http://localhost:4567/kb/graql?keyspace=grakn&infer=false&query=match%20%24x%20isa%20person;%20limit%201;"
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
curl -X POST -H "Accept:application/text" "http://localhost:4567/kb/graql?keyspace=grakn&infer=false" --data 'insert $x isa person;'
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
curl -X DELETE "http://localhost:4567/kb/graql?keyspace=grakn&infer=false" --data 'match $x isa parentship; delete $x;'
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
curl -X DELETE "http://localhost:4567/kb/graql?keyspace=grakn&infer=false" --data 'match $x isa parentship; delete $x;'
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

## Feedback
We hope you find our documentation helpful, but if you need more information, please don't hesitate to contact us using our [discussion forums](http://discuss.grakn.ai) or through our main [website](http://www.grakn.ai).

java_library(
    name = "janusgraph-cassandra",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-cassandra"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax-cassandra",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax-core",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax-recipes",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax-thrift",
        "//dependencies/maven/artifacts/commons-pool:commons-pool",
        "//dependencies/maven/artifacts/javax/validation:validation-api",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy-java",
        ":janusgraph-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph-core",
    exports = [
        "//dependencies/janusgraph:janusgraph-core"
    ],
    visibility = [
        "//visibility:public"
    ],
    runtime_deps = [
            "//dependencies/maven/artifacts/com/carrotsearch:hppc",
            "//dependencies/maven/artifacts/com/codahale/metrics:metrics-core",
            "//dependencies/maven/artifacts/com/codahale/metrics:metrics-ganglia",
            "//dependencies/maven/artifacts/com/codahale/metrics:metrics-graphite",
            "//dependencies/maven/artifacts/com/github/stephenc/high-scale-lib:high-scale-lib",
            "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//dependencies/maven/artifacts/commons-codec:commons-codec",
            "//dependencies/maven/artifacts/commons-collections:commons-collections",
            "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
            "//dependencies/maven/artifacts/commons-io:commons-io",
            "//dependencies/maven/artifacts/org/apache/commons:commons-text",
            "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
            "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-groovy",
            "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
            "//dependencies/maven/artifacts/org/glassfish:javax-json",
            "//dependencies/maven/artifacts/org/locationtech/jts:jts-core",
            "//dependencies/maven/artifacts/org/locationtech/spatial4j:spatial4j",
            "//dependencies/maven/artifacts/org/noggit:noggit",
            "//dependencies/maven/artifacts/org/reflections:reflections"
    ]
)



java_library(
    name = "janusgraph-es",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-es"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/org/antlr:antlr-runtime",
        "//dependencies/maven/artifacts/org/elasticsearch/client:elasticsearch-rest-client",
        ":janusgraph-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "janusgraph-hadoop",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-hadoop"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/com/datastax/cassandra:cassandra-driver-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/module:jackson-module-scala-2-11",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:spark-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-n3",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-ntriples",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-rdfxml",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-trig",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-trix",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame-rio-turtle",
        ":janusgraph-core",
        ":janusgraph-es",
        ":janusgraph-hbase"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph-hbase",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-hbase"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "janusgraph_cassandra",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_cassandra"
    ],
    runtime_deps = [
        "//dependencies/maven/com/netflix/astyanax:astyanax_cassandra",
        "//dependencies/maven/com/netflix/astyanax:astyanax_core",
        "//dependencies/maven/com/netflix/astyanax:astyanax_recipes",
        "//dependencies/maven/com/netflix/astyanax:astyanax_thrift",
        "//dependencies/maven/commons_pool:commons_pool",
        "//dependencies/maven/javax/validation:validation_api",
        "//dependencies/maven/net/jpountz/lz4:lz4",
        "//dependencies/maven/org/apache/cassandra:cassandra_all",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        ":janusgraph_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph_core",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_core"
    ],
    runtime_deps = [
        "//dependencies/maven/com/carrotsearch:hppc",
        "//dependencies/maven/com/codahale/metrics:metrics_core",
        "//dependencies/maven/com/codahale/metrics:metrics_ganglia",
        "//dependencies/maven/com/codahale/metrics:metrics_graphite",
        "//dependencies/maven/com/github/stephenc/high_scale_lib:high_scale_lib",
        "//dependencies/maven/com/google/code/findbugs:jsr305",
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/commons_codec:commons_codec",
        "//dependencies/maven/commons_collections:commons_collections",
        "//dependencies/maven/commons_configuration:commons_configuration",
        "//dependencies/maven/commons_io:commons_io",
        "//dependencies/maven/org/apache/commons:commons_text",
        "//dependencies/maven/org/apache/tinkerpop:gremlin_core",
        "//dependencies/maven/org/apache/tinkerpop:gremlin_groovy",
        "//dependencies/maven/org/apache/tinkerpop:tinkergraph_gremlin",
        "//dependencies/maven/org/glassfish:javax_json",
        "//dependencies/maven/org/locationtech/jts:jts_core",
        "//dependencies/maven/org/locationtech/spatial4j:spatial4j",
        "//dependencies/maven/org/noggit:noggit",
        "//dependencies/maven/org/reflections:reflections"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph_es",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_es"
    ],
    runtime_deps = [
        "//dependencies/maven/ch/qos/logback:logback_classic",
        "//dependencies/maven/org/antlr:antlr_runtime",
        "//dependencies/maven/org/elasticsearch/client:elasticsearch_rest_client",
        ":janusgraph_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "janusgraph_hadoop",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_hadoop"
    ],
    runtime_deps = [
        "//dependencies/maven/ch/qos/logback:logback_classic",
        "//dependencies/maven/com/datastax/cassandra:cassandra_driver_core",
        "//dependencies/maven/com/fasterxml/jackson/module:jackson_module_scala_2_11",
        "//dependencies/maven/org/apache/tinkerpop:hadoop_gremlin",
        "//dependencies/maven/org/apache/tinkerpop:spark_gremlin",
        "//dependencies/maven/org/apache/tinkerpop:tinkergraph_gremlin",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_n3",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_ntriples",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_rdfxml",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_trig",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_trix",
        "//dependencies/maven/org/openrdf/sesame:sesame_rio_turtle",
        ":janusgraph_core",
        ":janusgraph_es",
        ":janusgraph_hbase"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph_hbase",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_hbase"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



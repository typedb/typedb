java_library(
    name = "janusgraph_cassandra",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_cassandra"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax_cassandra",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax_core",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax_recipes",
        "//dependencies/maven/artifacts/com/netflix/astyanax:astyanax_thrift",
        "//dependencies/maven/artifacts/commons_pool:commons_pool",
        "//dependencies/maven/artifacts/javax/validation:validation_api",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra_all",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
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
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics_core",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics_ganglia",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics_graphite",
        "//dependencies/maven/artifacts/com/github/stephenc/high_scale_lib:high_scale_lib",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/commons_collections:commons_collections",
        "//dependencies/maven/artifacts/commons_configuration:commons_configuration",
        "//dependencies/maven/artifacts/commons_io:commons_io",
        "//dependencies/maven/artifacts/org/apache/commons:commons_text",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin_core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin_groovy",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph_gremlin",
        "//dependencies/maven/artifacts/org/glassfish:javax_json",
        "//dependencies/maven/artifacts/org/locationtech/jts:jts_core",
        "//dependencies/maven/artifacts/org/locationtech/spatial4j:spatial4j",
        "//dependencies/maven/artifacts/org/noggit:noggit",
        "//dependencies/maven/artifacts/org/reflections:reflections"
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
        "//dependencies/maven/artifacts/ch/qos/logback:logback_classic",
        "//dependencies/maven/artifacts/org/antlr:antlr_runtime",
        "//dependencies/maven/artifacts/org/elasticsearch/client:elasticsearch_rest_client",
        ":janusgraph_core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "janusgraph_hadoop",
    exports = [
        "//external:jar/org/janusgraph/janusgraph_hadoop"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback_classic",
        "//dependencies/maven/artifacts/com/datastax/cassandra:cassandra_driver_core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/module:jackson_module_scala_2_11",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop_gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:spark_gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph_gremlin",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_n3",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_ntriples",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_rdfxml",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_trig",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_trix",
        "//dependencies/maven/artifacts/org/openrdf/sesame:sesame_rio_turtle",
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
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



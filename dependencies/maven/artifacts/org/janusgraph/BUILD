java_library(
    name = "janusgraph-core",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/github/stephenc/high-scale-lib:high-scale-lib",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-collections:commons-collections",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-ganglia",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-graphite",
        "//dependencies/maven/artifacts/org/apache/commons:commons-text",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-groovy",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/glassfish:javax-json",
        "//dependencies/maven/artifacts/org/locationtech/jts:jts-core",
        "//dependencies/maven/artifacts/org/locationtech/spatial4j:spatial4j",
        "//dependencies/maven/artifacts/org/noggit:noggit",
        "//dependencies/maven/artifacts/org/reflections:reflections"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph-cql",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-cql"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/datastax/cassandra:cassandra-driver-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/io/netty:netty-handler",
        "//dependencies/maven/artifacts/io/vavr:vavr",
        ":janusgraph-core"
    ],
    visibility = [
        "//visibility:public"
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
        ":janusgraph-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



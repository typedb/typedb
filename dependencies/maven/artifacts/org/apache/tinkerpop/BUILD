java_library(
    name = "gremlin-core",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/jcabi:jcabi-manifests",
        "//dependencies/maven/artifacts/com/squareup:javapoet",
        "//dependencies/maven/artifacts/commons-collections:commons-collections",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
        "//dependencies/maven/artifacts/net/objecthunter:exp4j",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/javatuples:javatuples",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
        ":gremlin-shaded"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "gremlin-groovy",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-groovy"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/github/ben-manes/caffeine:caffeine",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/ivy:ivy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-groovysh-jar-indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-jar-indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-json-jar-indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-jsr223-jar-indy",
        "//dependencies/maven/artifacts/org/mindrot:jbcrypt",
        ":gremlin-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "gremlin-shaded",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-shaded"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/hadoop-gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/apache/commons:commons-compress",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-client",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy-java",
        ":gremlin-core",
        ":gremlin-groovy"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "spark-gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/spark-gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/org/apache/spark:spark-core-2-11",
        "//dependencies/maven/artifacts/org/scala-lang/modules:scala-xml-2-11",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy-java",
        ":gremlin-core",
        ":hadoop-gremlin"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "tinkergraph-gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/tinkergraph-gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        ":gremlin-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



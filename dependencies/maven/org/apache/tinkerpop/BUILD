java_library(
    name = "gremlin_core",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin_core"
    ],
    runtime_deps = [
        "//dependencies/maven/com/carrotsearch:hppc",
        "//dependencies/maven/com/jcabi:jcabi_manifests",
        "//dependencies/maven/com/squareup:javapoet",
        "//dependencies/maven/commons_collections:commons_collections",
        "//dependencies/maven/commons_configuration:commons_configuration",
        "//dependencies/maven/net/objecthunter:exp4j",
        "//dependencies/maven/org/javatuples:javatuples",
        "//dependencies/maven/org/slf4j:slf4j_api",
        "//dependencies/maven/org/yaml:snakeyaml",
        ":gremlin_shaded"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "gremlin_groovy",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin_groovy"
    ],
    runtime_deps = [
        "//dependencies/maven/com/github/ben_manes/caffeine:caffeine",
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/apache/ivy:ivy",
        "//dependencies/maven/org/codehaus/groovy:groovy_groovysh_jar_indy",
        "//dependencies/maven/org/codehaus/groovy:groovy_jar_indy",
        "//dependencies/maven/org/codehaus/groovy:groovy_json_jar_indy",
        "//dependencies/maven/org/codehaus/groovy:groovy_jsr223_jar_indy",
        "//dependencies/maven/org/mindrot:jbcrypt",
        ":gremlin_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "gremlin_shaded",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin_shaded"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "hadoop_gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/hadoop_gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/commons_codec:commons_codec",
        "//dependencies/maven/org/apache/hadoop:hadoop_client",
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        ":gremlin_core",
        ":gremlin_groovy"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "spark_gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/spark_gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/commons_lang:commons_lang",
        "//dependencies/maven/org/apache/spark:spark_core_2_10",
        "//dependencies/maven/org/scala_lang:scala_library",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        ":gremlin_core",
        ":gremlin_groovy",
        ":hadoop_gremlin"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "tinkergraph_gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/tinkergraph_gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/commons:commons_lang3",
        ":gremlin_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



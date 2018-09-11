java_library(
    name = "gremlin_core",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin_core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/jcabi:jcabi_manifests",
        "//dependencies/maven/artifacts/com/squareup:javapoet",
        "//dependencies/maven/artifacts/commons_collections:commons_collections",
        "//dependencies/maven/artifacts/commons_configuration:commons_configuration",
        "//dependencies/maven/artifacts/net/objecthunter:exp4j",
        "//dependencies/maven/artifacts/org/javatuples:javatuples",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
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
        "//dependencies/maven/artifacts/com/github/ben_manes/caffeine:caffeine",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/apache/ivy:ivy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_groovysh_jar_indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_jar_indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_json_jar_indy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_jsr223_jar_indy",
        "//dependencies/maven/artifacts/org/mindrot:jbcrypt",
        ":gremlin_core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "gremlin_shaded",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin_shaded"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop_gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/hadoop_gremlin"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop_client",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
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
        "//dependencies/maven/artifacts/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/artifacts/commons_lang:commons_lang",
        "//dependencies/maven/artifacts/org/apache/spark:spark_core_2_10",
        "//dependencies/maven/artifacts/org/scala_lang:scala_library",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
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
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        ":gremlin_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



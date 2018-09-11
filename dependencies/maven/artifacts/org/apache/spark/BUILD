java_library(
    name = "spark_core_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_core_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/clearspring/analytics:stream",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/ning:compress_lzf",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey_core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey_server",
        "//dependencies/maven/artifacts/com/twitter:chill_2_10",
        "//dependencies/maven/artifacts/com/twitter:chill_java",
        "//dependencies/maven/artifacts/com/typesafe/akka:akka_remote_2_10",
        "//dependencies/maven/artifacts/com/typesafe/akka:akka_slf4j_2_10",
        "//dependencies/maven/artifacts/commons_net:commons_net",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_graphite",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_json",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_jvm",
        "//dependencies/maven/artifacts/io/netty:netty_all",
        "//dependencies/maven/artifacts/net/java/dev/jets3t:jets3t",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/net/razorvine:pyrolite",
        "//dependencies/maven/artifacts/net/sf/py4j:py4j",
        "//dependencies/maven/artifacts/org/apache/avro:avro_mapred_jar_hadoop2",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/apache/commons:commons_math3",
        "//dependencies/maven/artifacts/org/apache/curator:curator_recipes",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop_client",
        "//dependencies/maven/artifacts/org/apache/ivy:ivy",
        "//dependencies/maven/artifacts/org/apache/mesos:mesos_jar_shaded_protobuf",
        "//dependencies/maven/artifacts/org/apache/xbean:xbean_asm5_shaded",
        "//dependencies/maven/artifacts/org/eclipse/jetty/orbit:javax_servlet",
        "//dependencies/maven/artifacts/org/json4s:json4s_jackson_2_10",
        "//dependencies/maven/artifacts/org/roaringbitmap:RoaringBitmap",
        "//dependencies/maven/artifacts/org/scala_lang:scala_library",
        "//dependencies/maven/artifacts/org/slf4j:jul_to_slf4j",
        "//dependencies/maven/artifacts/org/spark_project/spark:unused",
        "//dependencies/maven/artifacts/org/tachyonproject:tachyon_client",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
        "//dependencies/maven/artifacts/oro:oro",
        ":spark_launcher_2_10",
        ":spark_network_common_2_10",
        ":spark_network_shuffle_2_10",
        ":spark_unsafe_2_10"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "spark_launcher_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_launcher_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark_network_common_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_network_common_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/io/netty:netty_all",
        "//dependencies/maven/artifacts/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark_network_shuffle_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_network_shuffle_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni_all",
        "//dependencies/maven/artifacts/org/spark_project/spark:unused",
        ":spark_network_common_2_10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark_unsafe_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_unsafe_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/twitter:chill_2_10",
        "//dependencies/maven/artifacts/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



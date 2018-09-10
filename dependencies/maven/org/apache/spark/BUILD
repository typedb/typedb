java_library(
    name = "spark_core_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_core_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/clearspring/analytics:stream",
        "//dependencies/maven/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/com/google/code/findbugs:jsr305",
        "//dependencies/maven/com/ning:compress_lzf",
        "//dependencies/maven/com/sun/jersey:jersey_core",
        "//dependencies/maven/com/sun/jersey:jersey_server",
        "//dependencies/maven/com/twitter:chill_2_10",
        "//dependencies/maven/com/twitter:chill_java",
        "//dependencies/maven/com/typesafe/akka:akka_remote_2_10",
        "//dependencies/maven/com/typesafe/akka:akka_slf4j_2_10",
        "//dependencies/maven/commons_net:commons_net",
        "//dependencies/maven/io/dropwizard/metrics:metrics_graphite",
        "//dependencies/maven/io/dropwizard/metrics:metrics_json",
        "//dependencies/maven/io/dropwizard/metrics:metrics_jvm",
        "//dependencies/maven/io/netty:netty_all",
        "//dependencies/maven/net/java/dev/jets3t:jets3t",
        "//dependencies/maven/net/jpountz/lz4:lz4",
        "//dependencies/maven/net/razorvine:pyrolite",
        "//dependencies/maven/net/sf/py4j:py4j",
        "//dependencies/maven/org/apache/avro:avro_mapred_jar_hadoop2",
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/apache/commons:commons_math3",
        "//dependencies/maven/org/apache/curator:curator_recipes",
        "//dependencies/maven/org/apache/hadoop:hadoop_client",
        "//dependencies/maven/org/apache/ivy:ivy",
        "//dependencies/maven/org/apache/mesos:mesos_jar_shaded_protobuf",
        "//dependencies/maven/org/apache/xbean:xbean_asm5_shaded",
        "//dependencies/maven/org/eclipse/jetty/orbit:javax_servlet",
        "//dependencies/maven/org/json4s:json4s_jackson_2_10",
        "//dependencies/maven/org/roaringbitmap:RoaringBitmap",
        "//dependencies/maven/org/scala_lang:scala_library",
        "//dependencies/maven/org/slf4j:jul_to_slf4j",
        "//dependencies/maven/org/spark_project/spark:unused",
        "//dependencies/maven/org/tachyonproject:tachyon_client",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        "//dependencies/maven/oro:oro",
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
        "//dependencies/maven/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "spark_network_common_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_network_common_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/code/findbugs:jsr305",
        "//dependencies/maven/io/netty:netty_all",
        "//dependencies/maven/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "spark_network_shuffle_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_network_shuffle_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/fasterxml/jackson/core:jackson_annotations",
        "//dependencies/maven/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/org/fusesource/leveldbjni:leveldbjni_all",
        "//dependencies/maven/org/spark_project/spark:unused",
        ":spark_network_common_2_10"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "spark_unsafe_2_10",
    exports = [
        "//external:jar/org/apache/spark/spark_unsafe_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/code/findbugs:jsr305",
        "//dependencies/maven/com/twitter:chill_2_10",
        "//dependencies/maven/org/spark_project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



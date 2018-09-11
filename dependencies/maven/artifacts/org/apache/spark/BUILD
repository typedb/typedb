java_library(
    name = "spark-core-2-10",
    exports = [
        "//external:jar/org/apache/spark/spark-core-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/clearspring/analytics:stream",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/ning:compress-lzf",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-server",
        "//dependencies/maven/artifacts/com/twitter:chill-2-10",
        "//dependencies/maven/artifacts/com/twitter:chill-java",
        "//dependencies/maven/artifacts/com/typesafe/akka:akka-remote-2-10",
        "//dependencies/maven/artifacts/com/typesafe/akka:akka-slf4j-2-10",
        "//dependencies/maven/artifacts/commons-net:commons-net",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-graphite",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-json",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-jvm",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/net/java/dev/jets3t:jets3t",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/net/razorvine:pyrolite",
        "//dependencies/maven/artifacts/net/sf/py4j:py4j",
        "//dependencies/maven/artifacts/org/apache/avro:avro-mapred-jar-hadoop2",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/apache/curator:curator-recipes",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-client",
        "//dependencies/maven/artifacts/org/apache/ivy:ivy",
        "//dependencies/maven/artifacts/org/apache/mesos:mesos-jar-shaded-protobuf",
        "//dependencies/maven/artifacts/org/apache/xbean:xbean-asm5-shaded",
        "//dependencies/maven/artifacts/org/eclipse/jetty/orbit:javax-servlet",
        "//dependencies/maven/artifacts/org/json4s:json4s-jackson-2-10",
        "//dependencies/maven/artifacts/org/roaringbitmap:RoaringBitmap",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/slf4j:jul-to-slf4j",
        "//dependencies/maven/artifacts/org/spark-project/spark:unused",
        "//dependencies/maven/artifacts/org/tachyonproject:tachyon-client",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy-java",
        "//dependencies/maven/artifacts/oro:oro",
        ":spark-launcher-2-10",
        ":spark-network-common-2-10",
        ":spark-network-shuffle-2-10",
        ":spark-unsafe-2-10"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "spark-launcher-2-10",
    exports = [
        "//external:jar/org/apache/spark/spark-launcher-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/spark-project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark-network-common-2-10",
    exports = [
        "//external:jar/org/apache/spark/spark-network-common-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/org/spark-project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark-network-shuffle-2-10",
    exports = [
        "//external:jar/org/apache/spark/spark-network-shuffle-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni-all",
        "//dependencies/maven/artifacts/org/spark-project/spark:unused",
        ":spark-network-common-2-10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "spark-unsafe-2-10",
    exports = [
        "//external:jar/org/apache/spark/spark-unsafe-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/twitter:chill-2-10",
        "//dependencies/maven/artifacts/org/spark-project/spark:unused"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



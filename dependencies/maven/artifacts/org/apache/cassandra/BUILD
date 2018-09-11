java_library(
    name = "cassandra_all",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra_all"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/addthis/metrics:reporter_config3",
        "//dependencies/maven/artifacts/com/boundary:high_scale_lib",
        "//dependencies/maven/artifacts/com/clearspring/analytics:stream",
        "//dependencies/maven/artifacts/com/github/ben_manes/caffeine:caffeine",
        "//dependencies/maven/artifacts/com/github/jbellis:jamm",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/googlecode/concurrentlinkedhashmap:concurrentlinkedhashmap_lru",
        "//dependencies/maven/artifacts/com/googlecode/json_simple:json_simple",
        "//dependencies/maven/artifacts/com/ning:compress_lzf",
        "//dependencies/maven/artifacts/com/thinkaurelius/thrift:thrift_server",
        "//dependencies/maven/artifacts/commons_cli:commons_cli",
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/io/airlift:airline",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_core",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_jvm",
        "//dependencies/maven/artifacts/io/netty:netty_all",
        "//dependencies/maven/artifacts/joda_time:joda_time",
        "//dependencies/maven/artifacts/net/java/dev/jna:jna",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/org/antlr:antlr",
        "//dependencies/maven/artifacts/org/antlr:antlr_runtime",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/apache/commons:commons_math3",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/caffinitas/ohc:ohc_core",
        "//dependencies/maven/artifacts/org/caffinitas/ohc:ohc_core_j8",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/artifacts/org/eclipse/jdt/core/compiler:ecj",
        "//dependencies/maven/artifacts/org/fusesource:sigar",
        "//dependencies/maven/artifacts/org/jctools:jctools_core",
        "//dependencies/maven/artifacts/org/mindrot:jbcrypt",
        "//dependencies/maven/artifacts/org/ow2/asm:asm",
        "//dependencies/maven/artifacts/org/slf4j:jcl_over_slf4j",
        "//dependencies/maven/artifacts/org/slf4j:log4j_over_slf4j",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
        ":cassandra_thrift"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "cassandra_thrift",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra_thrift"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/github/rholder:snowball_stemmer",
        "//dependencies/maven/artifacts/com/googlecode/concurrent_trees:concurrent_trees",
        "//dependencies/maven/artifacts/de/jflex:jflex",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/slf4j:jcl_over_slf4j",
        "//dependencies/maven/artifacts/org/slf4j:log4j_over_slf4j",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



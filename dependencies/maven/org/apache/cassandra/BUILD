java_library(
    name = "cassandra_all",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra_all"
    ],
    runtime_deps = [
        "//dependencies/maven/com/addthis/metrics:reporter_config3",
        "//dependencies/maven/com/boundary:high_scale_lib",
        "//dependencies/maven/com/clearspring/analytics:stream",
        "//dependencies/maven/com/github/ben_manes/caffeine:caffeine",
        "//dependencies/maven/com/github/jbellis:jamm",
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/com/googlecode/concurrentlinkedhashmap:concurrentlinkedhashmap_lru",
        "//dependencies/maven/com/googlecode/json_simple:json_simple",
        "//dependencies/maven/com/ning:compress_lzf",
        "//dependencies/maven/com/thinkaurelius/thrift:thrift_server",
        "//dependencies/maven/commons_cli:commons_cli",
        "//dependencies/maven/commons_codec:commons_codec",
        "//dependencies/maven/io/airlift:airline",
        "//dependencies/maven/io/dropwizard/metrics:metrics_core",
        "//dependencies/maven/io/dropwizard/metrics:metrics_jvm",
        "//dependencies/maven/io/netty:netty_all",
        "//dependencies/maven/joda_time:joda_time",
        "//dependencies/maven/net/java/dev/jna:jna",
        "//dependencies/maven/net/jpountz/lz4:lz4",
        "//dependencies/maven/org/antlr:antlr",
        "//dependencies/maven/org/antlr:antlr_runtime",
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/apache/commons:commons_math3",
        "//dependencies/maven/org/apache/thrift:libthrift",
        "//dependencies/maven/org/caffinitas/ohc:ohc_core",
        "//dependencies/maven/org/caffinitas/ohc:ohc_core_j8",
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/org/eclipse/jdt/core/compiler:ecj",
        "//dependencies/maven/org/fusesource:sigar",
        "//dependencies/maven/org/jctools:jctools_core",
        "//dependencies/maven/org/mindrot:jbcrypt",
        "//dependencies/maven/org/ow2/asm:asm",
        "//dependencies/maven/org/slf4j:jcl_over_slf4j",
        "//dependencies/maven/org/slf4j:log4j_over_slf4j",
        "//dependencies/maven/org/slf4j:slf4j_api",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        "//dependencies/maven/org/yaml:snakeyaml",
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
        "//dependencies/maven/com/carrotsearch:hppc",
        "//dependencies/maven/com/github/rholder:snowball_stemmer",
        "//dependencies/maven/com/googlecode/concurrent_trees:concurrent_trees",
        "//dependencies/maven/de/jflex:jflex",
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/apache/thrift:libthrift",
        "//dependencies/maven/org/slf4j:jcl_over_slf4j",
        "//dependencies/maven/org/slf4j:log4j_over_slf4j",
        "//dependencies/maven/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



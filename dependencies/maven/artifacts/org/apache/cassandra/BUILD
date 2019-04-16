java_library(
    name = "cassandra-all",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra-all"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/boundary:high-scale-lib",
        "//dependencies/maven/artifacts/com/clearspring/analytics:stream",
        "//dependencies/maven/artifacts/com/github/ben-manes/caffeine:caffeine",
        "//dependencies/maven/artifacts/com/github/jbellis:jamm",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/googlecode/concurrentlinkedhashmap:concurrentlinkedhashmap-lru",
        "//dependencies/maven/artifacts/com/googlecode/json-simple:json-simple",
        "//dependencies/maven/artifacts/com/ning:compress-lzf",
        "//dependencies/maven/artifacts/com/thinkaurelius/thrift:thrift-server",
        "//dependencies/maven/artifacts/commons-cli:commons-cli",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/io/airlift:airline",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-jvm",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/joda-time:joda-time",
        "//dependencies/maven/artifacts/net/java/dev/jna:jna",
        "//dependencies/maven/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/maven/artifacts/org/antlr:antlr",
        "//dependencies/maven/artifacts/org/antlr:antlr-runtime",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/caffinitas/ohc:ohc-core",
        "//dependencies/maven/artifacts/org/caffinitas/ohc:ohc-core-j8",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/maven/artifacts/org/fusesource:sigar",
        "//dependencies/maven/artifacts/org/jctools:jctools-core",
        "//dependencies/maven/artifacts/org/mindrot:jbcrypt",
        "//dependencies/maven/artifacts/org/ow2/asm:asm",
        "//dependencies/maven/artifacts/org/slf4j:jcl-over-slf4j",
        "//dependencies/maven/artifacts/org/slf4j:log4j-over-slf4j",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy-java",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
        ":cassandra-thrift"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "cassandra-thrift",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra-thrift"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/carrotsearch:hppc",
        "//dependencies/maven/artifacts/com/github/rholder:snowball-stemmer",
        "//dependencies/maven/artifacts/com/googlecode/concurrent-trees:concurrent-trees",
        "//dependencies/maven/artifacts/de/jflex:jflex",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/slf4j:jcl-over-slf4j",
        "//dependencies/maven/artifacts/org/slf4j:log4j-over-slf4j",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



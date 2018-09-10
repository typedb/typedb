java_library(
    name = "astyanax_cassandra",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_cassandra"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_codec:commons_codec",
        "//dependencies/maven/org/apache/servicemix/bundles:org_apache_servicemix_bundles_commons_csv",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/org/codehaus/jettison:jettison",
        "//dependencies/maven/org/slf4j:slf4j_api",
        "//dependencies/maven/org/xerial/snappy:snappy_java",
        ":astyanax_cassandra_all_shaded",
        ":astyanax_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "astyanax_cassandra_all_shaded",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_cassandra_all_shaded"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/cassandra:cassandra_all"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "astyanax_core",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_core"
    ],
    runtime_deps = [
        "//dependencies/maven/com/eaio/uuid:uuid",
        "//dependencies/maven/com/github/stephenc/high_scale_lib:high_scale_lib",
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/commons_lang:commons_lang",
        "//dependencies/maven/joda_time:joda_time",
        "//dependencies/maven/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "astyanax_recipes",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_recipes"
    ],
    runtime_deps = [
        ":astyanax_cassandra",
        ":astyanax_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "astyanax_thrift",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_thrift"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/cassandra:cassandra_thrift",
        ":astyanax_cassandra",
        ":astyanax_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



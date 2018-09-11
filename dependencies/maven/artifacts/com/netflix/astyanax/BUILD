java_library(
    name = "astyanax_cassandra",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_cassandra"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/org/apache/servicemix/bundles:org_apache_servicemix_bundles_commons_csv",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/artifacts/org/codehaus/jettison:jettison",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        "//dependencies/maven/artifacts/org/xerial/snappy:snappy_java",
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
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra_all"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "astyanax_core",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax_core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/eaio/uuid:uuid",
        "//dependencies/maven/artifacts/com/github/stephenc/high_scale_lib:high_scale_lib",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons_lang:commons_lang",
        "//dependencies/maven/artifacts/joda_time:joda_time",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api"
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
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra_thrift",
        ":astyanax_cassandra",
        ":astyanax_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



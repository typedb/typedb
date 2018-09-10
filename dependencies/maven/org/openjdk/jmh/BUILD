java_library(
    name = "jmh_core",
    exports = [
        "//external:jar/org/openjdk/jmh/jmh_core"
    ],
    runtime_deps = [
        "//dependencies/maven/net/sf/jopt_simple:jopt_simple",
        "//dependencies/maven/org/apache/commons:commons_math3"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jmh_generator_annprocess",
    exports = [
        "//external:jar/org/openjdk/jmh/jmh_generator_annprocess"
    ],
    runtime_deps = [
        ":jmh_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



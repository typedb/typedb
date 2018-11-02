java_library(
    name = "jmh-core",
    exports = [
        "//external:jar/org/openjdk/jmh/jmh-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/net/sf/jopt-simple:jopt-simple",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jmh-generator-annprocess",
    exports = [
        "//external:jar/org/openjdk/jmh/jmh-generator-annprocess"
    ],
    runtime_deps = [
        ":jmh-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



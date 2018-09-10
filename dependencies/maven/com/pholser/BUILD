java_library(
    name = "junit_quickcheck_core",
    exports = [
        "//external:jar/com/pholser/junit_quickcheck_core"
    ],
    runtime_deps = [
        "//dependencies/maven/junit:junit",
        "//dependencies/maven/ognl:ognl",
        "//dependencies/maven/org/javaruntype:javaruntype",
        "//dependencies/maven/ru/vyarus:generics_resolver"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "junit_quickcheck_generators",
    exports = [
        "//external:jar/com/pholser/junit_quickcheck_generators"
    ],
    runtime_deps = [
        "//dependencies/maven/junit:junit",
        ":junit_quickcheck_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



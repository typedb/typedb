java_library(
    name = "junit_quickcheck_core",
    exports = [
        "//external:jar/com/pholser/junit_quickcheck_core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/ognl:ognl",
        "//dependencies/maven/artifacts/org/javaruntype:javaruntype",
        "//dependencies/maven/artifacts/ru/vyarus:generics_resolver"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "junit_quickcheck_generators",
    exports = [
        "//external:jar/com/pholser/junit_quickcheck_generators"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        ":junit_quickcheck_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



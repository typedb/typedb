java_library(
    name = "junit-quickcheck-core",
    exports = [
        "//external:jar/com/pholser/junit-quickcheck-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/ognl:ognl",
        "//dependencies/maven/artifacts/org/javaruntype:javaruntype",
        "//dependencies/maven/artifacts/ru/vyarus:generics-resolver"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "junit-quickcheck-generators",
    exports = [
        "//external:jar/com/pholser/junit-quickcheck-generators"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        ":junit-quickcheck-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



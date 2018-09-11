java_library(
    name = "commons_compiler",
    exports = [
        "//external:jar/org/codehaus/janino/commons_compiler"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "janino",
    exports = [
        "//external:jar/org/codehaus/janino/janino"
    ],
    runtime_deps = [
        ":commons_compiler"
    ],
    visibility = [
        "//visibility:public"
    ]
)



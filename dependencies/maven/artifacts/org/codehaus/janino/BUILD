java_library(
    name = "commons-compiler",
    exports = [
        "//external:jar/org/codehaus/janino/commons-compiler"
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
        ":commons-compiler"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "akka-actor-2-10",
    exports = [
        "//external:jar/com/typesafe/akka/akka-actor-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/typesafe:config",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "akka-remote-2-10",
    exports = [
        "//external:jar/com/typesafe/akka/akka-remote-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/io/netty:netty",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/uncommons/maths:uncommons-maths",
        ":akka-actor-2-10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "akka-slf4j-2-10",
    exports = [
        "//external:jar/com/typesafe/akka/akka-slf4j-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        ":akka-actor-2-10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



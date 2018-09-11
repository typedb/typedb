java_library(
    name = "akka_actor_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_actor_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/typesafe:config",
        "//dependencies/maven/artifacts/org/scala_lang:scala_library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "akka_remote_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_remote_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf_java",
        "//dependencies/maven/artifacts/io/netty:netty",
        "//dependencies/maven/artifacts/org/scala_lang:scala_library",
        "//dependencies/maven/artifacts/org/uncommons/maths:uncommons_maths",
        ":akka_actor_2_10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "akka_slf4j_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_slf4j_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/scala_lang:scala_library",
        ":akka_actor_2_10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



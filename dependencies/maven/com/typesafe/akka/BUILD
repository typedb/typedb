java_library(
    name = "akka_actor_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_actor_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/typesafe:config",
        "//dependencies/maven/org/scala_lang:scala_library"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "akka_remote_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_remote_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/protobuf:protobuf_java",
        "//dependencies/maven/io/netty:netty",
        "//dependencies/maven/org/scala_lang:scala_library",
        "//dependencies/maven/org/uncommons/maths:uncommons_maths",
        ":akka_actor_2_10"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "akka_slf4j_2_10",
    exports = [
        "//external:jar/com/typesafe/akka/akka_slf4j_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/org/scala_lang:scala_library",
        ":akka_actor_2_10"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



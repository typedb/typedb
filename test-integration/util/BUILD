
java_library(
    name = "grakn-test-util",
    srcs = ["GraknTestUtil.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
      "//server",
      "//grakn-graql",
      "//dependencies/maven/artifacts/com/sparkjava:spark-core"
    ]
)

java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
      "//grakn-graql",
      "//dependencies/maven/artifacts/commons-collections:commons-collections",
      "//dependencies/maven/artifacts/junit:junit"
    ]
)
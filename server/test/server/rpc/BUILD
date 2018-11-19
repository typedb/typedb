
#TODO this test should be probably be re-written as it is something in between unit and integration test
java_test(
    name = "server-rpc-test",
    srcs = ["ServerRPCTest.java"],
    test_class = "grakn.core.server.rpc.ServerRPCTest",
    deps = [
        "//common",
        "//server",
        "//client-java",
        "//protocol:protocol-java",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "small"
)
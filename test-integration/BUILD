java_library(
    name = "test-integration",
    srcs = glob(["test/**/*.java"])
)

test_suite(
    name = "test-integration",
    tests = [
        "//test-integration/test/server:attribute-deduplicator-it",
        "//test-integration/test/server:server-start-it"
    ]
)
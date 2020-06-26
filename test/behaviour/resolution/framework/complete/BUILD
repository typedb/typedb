exports_files(["completion_schema.gql"])

java_library(
    name = "complete",
    srcs = [
        "Completer.java",
        "SchemaManager.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//test/behaviour/resolution/framework/common:common",
        "//test/behaviour/resolution/framework/resolve:resolve",
        "//kb/server:server",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/complete:completion_schema.gql",
    ],
)

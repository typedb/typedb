exports_files(["completion_schema.gql"])

java_library(
    name = "complete",
    srcs = glob([
        "*.java",
        ]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//test/behaviour/resolution/common:common",
        "//test/behaviour/resolution/resolve:resolve",
        "//kb/server:server",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/complete:completion_schema.gql",
    ],
)

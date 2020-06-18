exports_files(["completion_schema.gql"])

java_library(
    name = "complete",
    srcs = glob([
        "*.java",
        ]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//resolution/common:common",
        "//resolution/resolve:resolve",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//resolution/complete:completion_schema.gql",
    ],
)

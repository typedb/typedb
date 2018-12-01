load("@org_pubref_rules_proto//python:compile.bzl", "python_grpc_compile")
load("@io_bazel_rules_python//python:python.bzl", "py_library")
load("@graknlabs_rules_deployment//pip:rules.bzl", "py_replace_imports")


exports_files([
    "__init__.py",
    "service/Keyspace/KeyspaceService.py",
    "service/Session/TransactionService.py",
    "service/Session/util/enums.py",
    "service/Session/util/RequestBuilder.py",
    "service/Session/util/ResponseReader.py",
    "service/Session/Concept/ConceptFactory.py",
    "service/Session/Concept/BaseTypeMapping.py",
    "service/Session/Concept/Concept.py",
    "exception/GraknError.py"
])

python_grpc_compile(
    name = "rpc_raw",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ],
     visibility = ["//visibility:public"]
)

py_replace_imports(
    name = "rpc",
    src = ":rpc_raw",
    original_name = "rpc_raw",
    output_name = "rpc",
    original_package = "protocol",
    output_package = "grakn.rpc.protocol"
)

py_library(
    name = "protocol_python",
    srcs = [":rpc"],
    visibility = ["//visibility:public"]
)

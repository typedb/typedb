genrule(
    name = "distribution",
    srcs = [
        "//:grakn",
        "//console:console-binary_deploy.jar",
        "//dashboard:assets",
        "//server:conf/logback.xml",
        "//server:conf/grakn.properties",
        "//server:jar-server_deploy.jar",
        "//server:src/services/cassandra/cassandra.yaml",
        "//server:src/services/cassandra/logback.xml",
        "//server:src/services/grakn/grakn-core-ascii.txt",
    ],
    outs = ["//:dist/grakn-core-all.zip"],
    cmd = "$(location distribution-packager.sh) $(location //:dist/grakn-core-all.zip) $(location //:grakn) $(location //console:console-binary_deploy.jar) $(location //server:jar-server_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:src/services/cassandra/logback.xml) $(location //server:src/services/cassandra/cassandra.yaml) \"dashboard/static/\" $(locations //dashboard:assets)",
    tools = ["distribution-packager.sh"],
    visibility = ["//visibility:public"]
)

exports_files(["grakn"], visibility = ["//visibility:public"])

test_suite(
    name = "test-unit",
    tests = [
        "//server:test-unit",
        "//grakn-graql:test-unit",
        "//client-java:test-unit"
    ]
)
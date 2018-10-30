genrule(
    name = "distribution",
    srcs = [
        "//:grakn",
        "//console:console-binary_deploy.jar",
        "//dashboard:assets",
        "//server:conf/logback.xml",
        "//server:conf/grakn.properties",
        "//server:server-binary_deploy.jar",
        "//server:services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt",
    ],
    outs = ["//:dist/grakn-core-all.zip"],
    cmd = "$(location distribution.sh) $(location //:dist/grakn-core-all.zip) $(location //:grakn) $(location //console:console-binary_deploy.jar) $(location //server:server-binary_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:services/cassandra/logback.xml) $(location //server:services/cassandra/cassandra.yaml) \"dashboard/static/\" $(locations //dashboard:assets)",
    tools = ["distribution.sh"],
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
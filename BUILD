genrule(
    name = "distribution-all",
    srcs = [
        "grakn-core",
        "jar-console_deploy.jar",
        "jar-server_deploy.jar", "//server:conf/grakn.properties", "//server:conf/logback.xml", "//server:src/services/cassandra/logback.xml", "//server:src/services/cassandra/cassandra.yaml", "//dashboard:assets"
    ],
    outs = ["//:dist/grakn-core-all.zip"],
    cmd = "$(location all-packager.sh) $(location //:dist/grakn-core-all.zip) $(location grakn-core) $(location jar-console_deploy.jar) $(location jar-server_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:src/services/cassandra/logback.xml) $(location //server:src/services/cassandra/cassandra.yaml) \"dashboard/static/\" $(locations //dashboard:assets)",
    tools = ["all-packager.sh"],
    visibility = ["//visibility:public"]
)

exports_files(["grakn-core"], visibility = ["//visibility:public"])

test_suite(
    name = "test-unit",
    tests = [
        "//server:test-unit",
        "//grakn-graql:test-unit",
        "//client-java:test-unit"
    ]
)
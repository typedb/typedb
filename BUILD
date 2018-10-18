java_binary(
    name = "jar-console",
    main_class = "ai.grakn.core.console.Graql",
    runtime_deps = ["//console:console"]
)

genrule(
    name = "distribution-console",
    srcs = ["grakn-core", "jar-console_deploy.jar", "//console:conf/logback.xml"],
    outs = ["//:dist/grakn-core-console.zip"],
    cmd  = "$(location console-packager.sh) $(location //:dist/grakn-core-console.zip) $(location grakn-core) $(location jar-console_deploy.jar) $(location //console:conf/logback.xml)",
    tools = ["console-packager.sh"]
)

java_binary(
    name = "jar-server",
    main_class = "ai.grakn.core.server.bootup.GraknBootup",
    runtime_deps = ["//server:server"]
)

genrule(
    name = "distribution-server",
    srcs = [
        "grakn-core",
        "jar-server_deploy.jar", "//server:conf/grakn.properties", "//server:conf/logback.xml", "//server:src/services/cassandra/logback.xml", "//server:src/services/cassandra/cassandra.yaml",
        "//dashboard:assets"
        ],
    outs = ["//:dist/grakn-core-server.zip"],
    cmd  = "$(location server-packager.sh) $(location //:dist/grakn-core-server.zip) $(location grakn-core) $(location jar-server_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:src/services/cassandra/logback.xml) $(location //server:src/services/cassandra/cassandra.yaml) \"dashboard/static/\" $(locations //dashboard:assets)",
    tools = ["server-packager.sh"]
)

genrule(
    name = "distribution-all",
    srcs = [
        "grakn-core",
        "jar-console_deploy.jar", "//console:conf/logback.xml",
        "jar-server_deploy.jar", "//server:conf/grakn.properties", "//server:conf/logback.xml", "//server:src/services/cassandra/logback.xml", "//server:src/services/cassandra/cassandra.yaml", "//dashboard:assets"
    ],
    outs = ["//:dist/grakn-core-all.zip"],
    cmd = "$(location all-packager.sh) $(location //:dist/grakn-core-all.zip) $(location grakn-core) $(location jar-console_deploy.jar) $(location //console:conf/logback.xml) $(location jar-server_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:src/services/cassandra/logback.xml) $(location //server:src/services/cassandra/cassandra.yaml) \"dashboard/static/\" $(locations //dashboard:assets)",
    tools = ["all-packager.sh"]
)

test_suite(
    name = "unit_tests",
    tests = [
        "//server:unit_tests",
        "//grakn-graql:unit_tests",
        "//client-java:unit_tests"
    ]
)
java_library(
    name = "hadoop-annotations",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-annotations"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "hadoop-auth",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-auth"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/log4j:log4j",
        "//dependencies/maven/artifacts/org/apache/curator:curator-framework",
        "//dependencies/maven/artifacts/org/apache/directory/server:apacheds-kerberos-codec",
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpclient",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-client",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-client"
    ],
    runtime_deps = [
        ":hadoop-annotations",
        ":hadoop-common",
        ":hadoop-hdfs",
        ":hadoop-mapreduce-client-app",
        ":hadoop-mapreduce-client-core",
        ":hadoop-mapreduce-client-jobclient",
        ":hadoop-yarn-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-common",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/code/gson:gson",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/com/jcraft:jsch",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-json",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-server",
        "//dependencies/maven/artifacts/commons-cli:commons-cli",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-collections:commons-collections",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
        "//dependencies/maven/artifacts/commons-httpclient:commons-httpclient",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        "//dependencies/maven/artifacts/commons-net:commons-net",
        "//dependencies/maven/artifacts/javax/servlet/jsp:jsp-api",
        "//dependencies/maven/artifacts/log4j:log4j",
        "//dependencies/maven/artifacts/net/java/dev/jets3t:jets3t",
        "//dependencies/maven/artifacts/org/apache/avro:avro",
        "//dependencies/maven/artifacts/org/apache/commons:commons-compress",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/apache/curator:curator-client",
        "//dependencies/maven/artifacts/org/apache/curator:curator-recipes",
        "//dependencies/maven/artifacts/org/apache/htrace:htrace-core",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty-util",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/xmlenc:xmlenc",
        ":hadoop-annotations",
        ":hadoop-auth"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "hadoop-hdfs",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-hdfs"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/commons-cli:commons-cli",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/org/apache/htrace:htrace-core",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni-all",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty-util",
        "//dependencies/maven/artifacts/xerces:xercesImpl",
        "//dependencies/maven/artifacts/xmlenc:xmlenc"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-mapreduce-client-app",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-mapreduce-client-app"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        ":hadoop-mapreduce-client-common",
        ":hadoop-mapreduce-client-shuffle"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-mapreduce-client-common",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-mapreduce-client-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        ":hadoop-mapreduce-client-core",
        ":hadoop-yarn-client",
        ":hadoop-yarn-common",
        ":hadoop-yarn-server-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-mapreduce-client-core",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-mapreduce-client-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/inject/extensions:guice-servlet",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/org/apache/avro:avro",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        ":hadoop-annotations",
        ":hadoop-yarn-common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "hadoop-mapreduce-client-jobclient",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-mapreduce-client-jobclient"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        ":hadoop-mapreduce-client-common",
        ":hadoop-mapreduce-client-shuffle"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-mapreduce-client-shuffle",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-mapreduce-client-shuffle"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni-all",
        ":hadoop-mapreduce-client-common",
        ":hadoop-yarn-server-common",
        ":hadoop-yarn-server-nodemanager"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-yarn-api",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-yarn-api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        ":hadoop-annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-yarn-client",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-yarn-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-cli:commons-cli",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        ":hadoop-yarn-api",
        ":hadoop-yarn-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-yarn-common",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-yarn-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/inject/extensions:guice-servlet",
        "//dependencies/maven/artifacts/com/google/inject:guice",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/com/sun/jersey/contribs:jersey-guice",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-client",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-json",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-server",
        "//dependencies/maven/artifacts/commons-cli:commons-cli",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        "//dependencies/maven/artifacts/javax/xml/bind:jaxb-api",
        "//dependencies/maven/artifacts/log4j:log4j",
        "//dependencies/maven/artifacts/org/apache/commons:commons-compress",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-jaxrs",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-xc",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty-util",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        ":hadoop-annotations",
        ":hadoop-yarn-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-yarn-server-common",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-yarn-server-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni-all",
        ":hadoop-yarn-api",
        ":hadoop-yarn-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hadoop-yarn-server-nodemanager",
    exports = [
        "//external:jar/org/apache/hadoop/hadoop-yarn-server-nodemanager"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/inject:guice",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/com/sun/jersey/contribs:jersey-guice",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-client",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-json",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/javax/xml/bind:jaxb-api",
        "//dependencies/maven/artifacts/org/codehaus/jettison:jettison",
        "//dependencies/maven/artifacts/org/fusesource/leveldbjni:leveldbjni-all",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty-util",
        ":hadoop-yarn-api",
        ":hadoop-yarn-common",
        ":hadoop-yarn-server-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



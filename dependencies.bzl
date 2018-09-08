#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#


def generated_maven_jars():
  # com.esotericsoftware.kryo:kryo:bundle:2.21
  native.maven_jar(
      name = "com_esotericsoftware_reflectasm_reflectasm",
      artifact = "com.esotericsoftware.reflectasm:reflectasm:1.07",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "761028ef46da8ec16a16b25ce942463eb1a9f3d5",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_recipes",
      artifact = "com.netflix.astyanax:astyanax-recipes:3.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "71cf76ec6ffd6142f3e73101b9d6a4c35a67cd2f",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_janusgraph_janusgraph_es",
      artifact = "org.janusgraph:janusgraph-es:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c726f76022d534cb7907e36663052e15bdcbb078",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  native.maven_jar(
      name = "com_google_auto_value_auto_value",
      artifact = "com.google.auto.value:auto-value:1.4.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "8172ebbd7970188aff304c8a420b9f17168f6f48",
  )


  # com.jamesmurty.utils:java-xmlbuilder:jar:1.0
  native.maven_jar(
      name = "net_iharder_base64",
      artifact = "net.iharder:base64:2.3.8",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7d2e2cea90cc51169fd02a35888820ab07f6d02f",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3
  native.maven_jar(
      name = "org_apache_spark_spark_core_2_10",
      artifact = "org.apache.spark:spark-core_2.10:1.6.1",
      repository = "http://central.maven.org/maven2/",
      sha1 = "902f3bf0e54e3b666ea0803bf2a9e81247619191",
  )


  # io.grpc:grpc-protobuf:jar:1.8.0
  native.maven_jar(
      name = "com_google_protobuf_protobuf_java_util",
      artifact = "com.google.protobuf:protobuf-java-util:3.4.0",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "96aba8ab71c16018c6adf66771ce15c6491bc0fe",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_spark_spark_network_shuffle_2_10",
      artifact = "org.apache.spark:spark-network-shuffle_2.10:1.6.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "950233ee31154a619a9bff8a4a3701ebdb17103f",
  )


  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 2.0.12
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT wanted version 3.11.3
  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 wanted version 2.0.12
  native.maven_jar(
      name = "org_apache_cassandra_cassandra_all",
      artifact = "org.apache.cassandra:cassandra-all:2.1.20",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "a47a6d396cd421aa4edecb876fcb19286f0bd481",
  )


  # org.apache.spark:spark-network-shuffle_2.10:jar:1.6.1 got requested version
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_spark_spark_network_common_2_10",
      artifact = "org.apache.spark:spark-network-common_2.10:1.6.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "de16d33941b761eb18faa6f04d39c7c12a70a4a0",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "net_java_dev_jets3t_jets3t",
      artifact = "net.java.dev.jets3t:jets3t:0.9.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "363851df5946a23be9539900518b152011df3455",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "javax_validation_validation_api",
      artifact = "javax.validation:validation-api:1.1.0.Final",
      repository = "https://repository.jboss.org/nexus/content/repositories/releases/",
      sha1 = "8613ae82954779d518631e05daa73a6a954817d5",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_boundary_high_scale_lib",
      artifact = "com.boundary:high-scale-lib:1.0.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7b44147cb2729e1724d2d46d7b932c56b65087f0",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.9.2
  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3 got requested version
  # org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13
  # net.java.dev.jets3t:jets3t:jar:0.9.1 wanted version 1.9.12
  native.maven_jar(
      name = "org_codehaus_jackson_jackson_core_asl",
      artifact = "org.codehaus.jackson:jackson-core-asl:1.9.13",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3c304d70f42f832e0a86d45bd437f692129299a4",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "org_ow2_asm_asm_tree",
      artifact = "org.ow2.asm:asm-tree:5.0.3",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "287749b48ba7162fb67c93a026d690b29f410bed",
  )


  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # com.fasterxml.jackson.module:jackson-module-paranamer:bundle:2.9.2 got requested version
  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.6.6
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_databind",
      artifact = "com.fasterxml.jackson.core:jackson-databind:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1d8d8cb7cf26920ba57fb61fa56da88cc123b21f",
  )


  # org.antlr:antlr:jar:3.5.2
  native.maven_jar(
      name = "org_antlr_ST4",
      artifact = "org.antlr:ST4:4.0.8",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0a1c55e974f8a94d78e2348fa6ff63f4fa1fae64",
  )


  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.6.6
  # com.fasterxml.jackson.core:jackson-databind:bundle:2.9.2
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_core",
      artifact = "com.fasterxml.jackson.core:jackson-core:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "aed20e50152a2f19adc1995c8d8f307c7efa414d",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_slf4j_simple",
      artifact = "org.slf4j:slf4j-simple:1.7.20",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0c554734bbc4ab98ae705e28ff5771e8da568111",
  )


  # pom.xml got requested version
  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:core.dashboard:jar:1.4.0-SNAPSHOT got requested version
  # org.janusgraph:janusgraph-es:jar:0.3.0 wanted version 1.1.2
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 wanted version 1.1.2
  native.maven_jar(
      name = "ch_qos_logback_logback_classic",
      artifact = "ch.qos.logback:logback-classic:1.2.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7c4f3c474fb2c041d8028740440937705ebb473a",
  )


  # com.thinkaurelius.thrift:thrift-server:jar:0.3.7
  native.maven_jar(
      name = "com_lmax_disruptor",
      artifact = "com.lmax:disruptor:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d68c363b12827c644bd60469827b862cad7dc0a2",
  )


  # com.google.protobuf:protobuf-java-util:bundle:3.4.0 wanted version 3.4.0
  # com.typesafe.akka:akka-remote_2.10:jar:2.3.11
  # io.grpc:grpc-protobuf:jar:1.8.0 wanted version 3.4.0
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT wanted version 3.4.0
  native.maven_jar(
      name = "com_google_protobuf_protobuf_java",
      artifact = "com.google.protobuf:protobuf-java:2.5.0",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "a10732c76bfacdbd633a7eb0f7968b1059a65dfa",
  )


  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "jline_jline",
      artifact = "jline:jline:2.12",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ce9062c6a125e0f9ad766032573c041ae8ecc986",
  )


  # com.netflix.hystrix:hystrix-core:jar:1.5.12
  native.maven_jar(
      name = "org_hdrhistogram_HdrHistogram",
      artifact = "org.hdrhistogram:HdrHistogram:2.1.9",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e4631ce165eb400edecfa32e03d3f1be53dee754",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 0.8.4
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_ning_compress_lzf",
      artifact = "com.ning:compress-lzf:1.0.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3e1495b0c532ebe58f1c8b1c5d9b3bdcc6c1504c",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "net_sf_py4j_py4j",
      artifact = "net.sf.py4j:py4j:0.9",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "23a3440b54e4ca6973022ff39ff73e0c662eb46d",
  )


  # com.codahale.metrics:metrics-graphite:bundle:3.0.1 got requested version
  # org.janusgraph:janusgraph-core:jar:0.3.0
  # com.codahale.metrics:metrics-ganglia:bundle:3.0.1 got requested version
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  native.maven_jar(
      name = "com_codahale_metrics_metrics_core",
      artifact = "com.codahale.metrics:metrics-core:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1e98427c7f6e53363b598e2943e50903ce4f3657",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_addthis_metrics_reporter_config",
      artifact = "com.addthis.metrics:reporter-config:2.1.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1f4c83a733634be5f550ed94ceb9475b17ce8ba6",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  native.maven_jar(
      name = "org_javatuples_javatuples",
      artifact = "org.javatuples:javatuples:1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "507312ac4b601204a72a83380badbca82683dd36",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "commons_net_commons_net",
      artifact = "commons-net:commons-net:2.2",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "07993c12f63c78378f8c90de4bc2ee62daa7ca3a",
  )


  # io.netty:netty-codec-socks:jar:4.0.30.Final
  # io.netty:netty-codec-http:jar:4.0.30.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_codec",
      artifact = "io.netty:netty-codec:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7ac41ffc6d7cf7d76db4b1f87373f44985314ec6",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_roaringbitmap_RoaringBitmap",
      artifact = "org.roaringbitmap:RoaringBitmap:0.5.11",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e6b04760ea1896fc36beea4f11b8649481bf5af7",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_tachyonproject_tachyon_client",
      artifact = "org.tachyonproject:tachyon-client:0.8.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "a0b0c068488d1a4eae155bd8ce2971cd89212a53",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3 wanted version 1.7.21
  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:core.dashboard:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  # ch.qos.logback:logback-classic:jar:1.2.3 wanted version 1.7.25
  # com.netflix.hystrix:hystrix-core:jar:1.5.12 wanted version 1.7.0
  # pom.xml got requested version
  # org.slf4j:slf4j-simple:jar:1.7.20 got requested version
  # com.netflix.archaius:archaius-core:jar:0.4.1 wanted version 1.6.4
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # org.slf4j:log4j-over-slf4j:jar:1.7.20 got requested version
  # org.slf4j:jcl-over-slf4j:jar:1.7.20 got requested version
  native.maven_jar(
      name = "org_slf4j_slf4j_api",
      artifact = "org.slf4j:slf4j-api:1.7.20",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "867d63093eff0a0cb527bf13d397d850af3dcae3",
  )


  # com.typesafe.akka:akka-actor_2.10:jar:2.3.11
  native.maven_jar(
      name = "com_typesafe_config",
      artifact = "com.typesafe:config:1.2.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f771f71fdae3df231bcd54d5ca2d57f0bf93f467",
  )


  # org.apache.tinkerpop:tinkergraph-gremlin:jar:3.3.3 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3 got requested version
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3 got requested version
  # org.janusgraph:janusgraph-core:jar:0.3.0 got requested version
  native.maven_jar(
      name = "org_apache_tinkerpop_gremlin_core",
      artifact = "org.apache.tinkerpop:gremlin-core:3.3.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "f46374ee04c48dbff1b56ac78ea8c5030bc993dd",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  native.maven_jar(
      name = "com_squareup_javapoet",
      artifact = "com.squareup:javapoet:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e858dc62ef484048540d27d36f3ec2177a3fa9b1",
  )


  # com.google.protobuf:protobuf-java-util:bundle:3.4.0
  native.maven_jar(
      name = "com_google_code_gson_gson",
      artifact = "com.google.code.gson:gson:2.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "751f548c85fa49f330cecbb1875893f971b33c4e",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  native.maven_jar(
      name = "com_jcabi_jcabi_manifests",
      artifact = "com.jcabi:jcabi-manifests:1.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e4f4488c0e3905c6fab287aca2569928fe1712df",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT wanted version 2.9.2
  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.6.6
  # com.fasterxml.jackson.core:jackson-databind:bundle:2.9.2
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2 wanted version 2.9.2
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_annotations",
      artifact = "com.fasterxml.jackson.core:jackson-annotations:2.9.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "07c10d545325e3a6e72e06381afe469fd40eb701",
  )


  # org.json4s:json4s-core_2.10:jar:3.2.10
  native.maven_jar(
      name = "org_json4s_json4s_ast_2_10",
      artifact = "org.json4s:json4s-ast_2.10:3.2.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "6b0269aac8115a624fe432e7d4ae3e9804240ca7",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  # org.apache.thrift:libthrift:pom:0.9.1 wanted version 4.2.5
  # org.elasticsearch.client:elasticsearch-rest-client:jar:6.0.1 wanted version 4.5.2
  # net.java.dev.jets3t:jets3t:jar:0.9.1 wanted version 4.3.2
  native.maven_jar(
      name = "org_apache_httpcomponents_httpclient",
      artifact = "org.apache.httpcomponents:httpclient:4.5.1",
      repository = "http://central.maven.org/maven2/",
      sha1 = "7e3cecc566df91338c6c67883b89ddd05a17db43",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "javax_ws_rs_jsr311_api",
      artifact = "javax.ws.rs:jsr311-api:1.1.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "59033da2a1afd56af1ac576750a8d0b1830d59e6",
  )


  # org.apache.spark:spark-network-common_2.10:jar:1.6.1
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT wanted version 4.1.29.Final
  # org.apache.spark:spark-core_2.10:jar:1.6.1 got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 4.0.44.Final
  native.maven_jar(
      name = "io_netty_netty_all",
      artifact = "io.netty:netty-all:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d83cee600887629344c2ca96e8fdeec8511c4eee",
  )


  # net.java.dev.jets3t:jets3t:jar:0.9.1
  native.maven_jar(
      name = "javax_mail_mail",
      artifact = "javax.mail:mail:1.4.7",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "9add058589d5d85adeb625859bf2c5eeaaedf12d",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "net_sf_supercsv_super_csv",
      artifact = "net.sf.supercsv:super-csv:2.1.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c6466dd0e28c034272b9f70a3f1896c03f1f2b27",
  )


  # io.grpc:grpc-protobuf:jar:1.8.0
  native.maven_jar(
      name = "io_grpc_grpc_protobuf_lite",
      artifact = "io.grpc:grpc-protobuf-lite:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3c40cd351e4206fad95f8dd612e0b94be4d1d1dd",
  )


  # io.grpc:grpc-netty:jar:1.8.0
  native.maven_jar(
      name = "io_netty_netty_handler_proxy",
      artifact = "io.netty:netty-handler-proxy:4.1.16.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e3007ed3368748ccdc35c1f38c7d6c089768373a",
  )


  # com.typesafe.akka:akka-remote_2.10:jar:2.3.11
  native.maven_jar(
      name = "io_netty_netty",
      artifact = "io.netty:netty:3.8.0.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "65e00948a33493081e4f6a8619d82ab3d4024af1",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "io_dropwizard_metrics_metrics_jvm",
      artifact = "io.dropwizard.metrics:metrics-jvm:3.1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ed364e77218e50fdcdebce4d982cb4d1f4a8c187",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_slf4j_jul_to_slf4j",
      artifact = "org.slf4j:jul-to-slf4j:1.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "35dd785e5e36c957da395091c837dd2ed57b7fa3",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  # com.netflix.archaius:archaius-core:jar:0.4.1 wanted version 1.8
  # org.janusgraph:janusgraph-core:jar:0.3.0 got requested version
  native.maven_jar(
      name = "commons_configuration_commons_configuration",
      artifact = "commons-configuration:commons-configuration:1.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "2b36e4adfb66d966c5aef2d73deb6be716389dc9",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_apache_commons_commons_text",
      artifact = "org.apache.commons:commons-text:1.0",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "71413afd09c3ca8b3a796bc2375ef154b0afa814",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "org_ow2_asm_asm",
      artifact = "org.ow2.asm:asm:5.0.3",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "dcc2193db20e19e1feca8b1240dbbc4e190824fa",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_googlecode_concurrentlinkedhashmap_concurrentlinkedhashmap_lru",
      artifact = "com.googlecode.concurrentlinkedhashmap:concurrentlinkedhashmap-lru:1.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "beb907bae0604fdc153cbcc2f0dc84d3ae35bf36",
  )


  # io.grpc:grpc-core:jar:1.8.0
  native.maven_jar(
      name = "io_opencensus_opencensus_contrib_grpc_metrics",
      artifact = "io.opencensus:opencensus-contrib-grpc-metrics:0.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "5e54d0e6dd946fe097e63ad68243e0006fbb1fbc",
  )


  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 wanted version 1.0.5
  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 wanted version 1.0.5-M3
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.0.5
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3
  native.maven_jar(
      name = "org_xerial_snappy_snappy_java",
      artifact = "org.xerial.snappy:snappy-java:1.1.1.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "33b6965e9364145972035c30a45a996aad2bf789",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "net_java_dev_jna_jna",
      artifact = "net.java.dev.jna:jna:4.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f11d386a05132f54a51c99085f016e496f345ea3",
  )


  # junit:junit:jar:4.12
  native.maven_jar(
      name = "org_hamcrest_hamcrest_core",
      artifact = "org.hamcrest:hamcrest-core:1.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "42a25dc3219429f0e5d060061f71acb49bf010a0",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_typesafe_akka_akka_slf4j_2_10",
      artifact = "com.typesafe.akka:akka-slf4j_2.10:2.3.11",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "bb54396770d81bde57e17024f4d34daf02fe125f",
  )


  # org.tachyonproject:tachyon-client:jar:0.8.2
  native.maven_jar(
      name = "org_tachyonproject_tachyon_underfs_local",
      artifact = "org.tachyonproject:tachyon-underfs-local:0.8.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "9dba21e4506e96d6688335465dcea8792e93bea2",
  )


  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "com_github_jnr_jnr_posix",
      artifact = "com.github.jnr:jnr-posix:3.0.27",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f7441d13187d93d59656ac8f800cba3043935b59",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "io_dropwizard_metrics_metrics_json",
      artifact = "io.dropwizard.metrics:metrics-json:3.1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "88d9e57e1ef6431109d4030c717cf5f927900fd9",
  )


  # io.grpc:grpc-core:jar:1.8.0
  native.maven_jar(
      name = "com_google_instrumentation_instrumentation_api",
      artifact = "com.google.instrumentation:instrumentation-api:0.4.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "41614af3429573dc02645d541638929d877945a2",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_googlecode_json_simple_json_simple",
      artifact = "com.googlecode.json-simple:json-simple:1.1.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c9ad4a0850ab676c5c64461a05ca524cdfff59f1",
  )


  # org.scala-lang:scala-reflect:jar:2.10.6 got requested version
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3 wanted version 2.10.5
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2
  native.maven_jar(
      name = "org_scala_lang_scala_library",
      artifact = "org.scala-lang:scala-library:2.10.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "421989aa8f95a05a4f894630aad96b8c7b828732",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "io_dropwizard_metrics_metrics_graphite",
      artifact = "io.dropwizard.metrics:metrics-graphite:3.1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "15a68399652c6123fe6e4c82ac4f0749e2eb6583",
  )


  # net.java.dev.jets3t:jets3t:jar:0.9.1
  native.maven_jar(
      name = "mx4j_mx4j",
      artifact = "mx4j:mx4j:3.0.2",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "47bf147f11b4a026263e1c96a1ea0e029f9e5ab6",
  )


  # com.netflix.astyanax:astyanax-core:jar:3.8.0
  native.maven_jar(
      name = "com_eaio_uuid_uuid",
      artifact = "com.eaio.uuid:uuid:3.2",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "77ba5105d949cd589aff75400d9f7d3676691a46",
  )


  # org.antlr:antlr4-runtime:jar:4.5
  native.maven_jar(
      name = "org_abego_treelayout_org_abego_treelayout_core",
      artifact = "org.abego.treelayout:org.abego.treelayout.core:1.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e31e79cba7a5414cf18fa69f3f0a2cf9ee997b61",
  )


  # org.reflections:reflections:jar:0.9.9-RC1
  native.maven_jar(
      name = "dom4j_dom4j",
      artifact = "dom4j:dom4j:1.6.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "5d3ccc056b6f056dbf0dddfdf43894b9065a8f94",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_stub",
      artifact = "io.grpc:grpc-stub:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "a9a213b2b23f0015d6820f715b51c4bdf9f45939",
  )


  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT wanted version 1.3
  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "commons_cli_commons_cli",
      artifact = "commons-cli:commons-cli:1.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "11c98b99ad538f2f67633afd4d7f4d98ecfbb408",
  )


  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3
  native.maven_jar(
      name = "org_apache_hadoop_hadoop_client",
      artifact = "org.apache.hadoop:hadoop-client:2.7.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "78a8a771e71268af7805169940817ac7ef9a378d",
  )


  # org.apache.cassandra:cassandra-thrift:jar:2.1.20 wanted version 0.9.2
  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0 got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 0.9.2
  # com.thinkaurelius.thrift:thrift-server:jar:0.3.7
  native.maven_jar(
      name = "org_apache_thrift_libthrift",
      artifact = "org.apache.thrift:libthrift:0.9.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "16c9cccf08caa385b5fc93934cb3216fe6ac6a72",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_github_jbellis_jamm",
      artifact = "com.github.jbellis:jamm:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "a08af6071e57d4eb5d13db780c7810f73b549f1a",
  )


  # com.github.jnr:jnr-posix:jar:3.0.27 got requested version
  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "com_github_jnr_jnr_ffi",
      artifact = "com.github.jnr:jnr-ffi:2.0.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f0968c5bb5a283ebda2df3604c2c1129d45196e3",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 wanted version 3.3.3
  native.maven_jar(
      name = "org_apache_tinkerpop_spark_gremlin",
      artifact = "org.apache.tinkerpop:spark-gremlin:3.2.5",
      repository = "http://central.maven.org/maven2/",
      sha1 = "455fae9b0c8657d5f5e674361a1242e26261a09a",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_antlr_antlr4_runtime",
      artifact = "org.antlr:antlr4-runtime:4.5",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "29e48af049f17dd89153b83a7ad5d01b3b4bcdda",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "commons_pool_commons_pool",
      artifact = "commons-pool:commons-pool:1.6",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "4572d589699f09d866a226a14b7f4323c6d8f040",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "net_jodah_failsafe",
      artifact = "net.jodah:failsafe:1.0.4",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0bae697c8cf83cfb8ee5c8a5cc1877f010e7a337",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_rdfxml",
      artifact = "org.openrdf.sesame:sesame-rio-rdfxml:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ac1c1049e3d7163f8a51d3f76049ca557dc7d719",
  )


  # org.janusgraph:janusgraph-es:jar:0.3.0
  native.maven_jar(
      name = "org_elasticsearch_client_elasticsearch_rest_client",
      artifact = "org.elasticsearch.client:elasticsearch-rest-client:6.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d1fd4aad149d91e91efc36a4a0fafc61a8d04cd6",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "org_ow2_asm_asm_analysis",
      artifact = "org.ow2.asm:asm-analysis:5.0.3",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "c7126aded0e8e13fed5f913559a0dd7b770a10f3",
  )


  # com.addthis.metrics:reporter-config:jar:2.1.0 wanted version 1.12
  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.11
  native.maven_jar(
      name = "org_yaml_snakeyaml",
      artifact = "org.yaml:snakeyaml:1.15",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3b132bea69e8ee099f416044970997bde80f4ea6",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 1.1.2
  native.maven_jar(
      name = "com_github_stephenc_high_scale_lib_high_scale_lib",
      artifact = "com.github.stephenc.high-scale-lib:high-scale-lib:1.1.4",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "093865cc75c598f67a7a98e259b2ecfceec9a132",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_code_findbugs_annotations",
      artifact = "com.google.code.findbugs:annotations:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "fc019a2216218990d64dfe756e7aa20f0069dea2",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  # org.janusgraph:janusgraph-core:jar:0.3.0 got requested version
  native.maven_jar(
      name = "com_carrotsearch_hppc",
      artifact = "com.carrotsearch:hppc:0.7.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "8b5057f74ea378c0150a1860874a3ebdcb713767",
  )


  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 2.3
  # org.tachyonproject:tachyon-client:jar:0.8.2
  # org.openrdf.sesame:sesame-rio-ntriples:jar:2.7.10 got requested version
  native.maven_jar(
      name = "commons_io_commons_io",
      artifact = "commons-io:commons-io:2.4",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "b1b6ea3b7e4aa4f492509a4952029cd8e48019ad",
  )


  # org.elasticsearch.client:elasticsearch-rest-client:jar:6.0.1 wanted version 1.10
  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3 wanted version 1.6
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.2
  # org.apache.httpcomponents:httpclient:jar:4.5.1
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 1.7
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 wanted version 1.6
  native.maven_jar(
      name = "commons_codec_commons_codec",
      artifact = "commons-codec:commons-codec:1.9",
      repository = "http://central.maven.org/maven2/",
      sha1 = "9ce04e34240f674bc72680f8b843b1457383161a",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_sun_jersey_jersey_core",
      artifact = "com.sun.jersey:jersey-core:1.9",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "8341846f18187013bb9e27e46b7ee00a6395daf4",
  )


  # dom4j:dom4j:jar:1.6.1
  native.maven_jar(
      name = "xml_apis_xml_apis",
      artifact = "xml-apis:xml-apis:1.0.b2",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "3136ca936f64c9d68529f048c2618bd356bf85c9",
  )


  # net.java.dev.jets3t:jets3t:jar:0.9.1
  native.maven_jar(
      name = "org_bouncycastle_bcprov_jdk15",
      artifact = "org.bouncycastle:bcprov-jdk15:1.46",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d726ceb2dcc711ef066cc639c12d856128ea1ef1",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_thrift",
      artifact = "com.netflix.astyanax:astyanax-thrift:3.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "43264bab44de99c0ff63f5b1fbe6e34591f07c1b",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_janusgraph_janusgraph_cassandra",
      artifact = "org.janusgraph:janusgraph-cassandra:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "aa2468d10049e27c2727be08ab3f3b9d579c2107",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 got requested version
  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0 got requested version
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 got requested version
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_core",
      artifact = "com.netflix.astyanax:astyanax-core:3.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "209a330c9856720b3ae8ccda8ccf819bc9cab362",
  )


  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-model:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-languages:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-api:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-datatypes:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_util",
      artifact = "org.openrdf.sesame:sesame-util:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "dcab3032f0ef3f0ab7c98c84bc8119ceeaa69dc3",
  )


  # io.netty:netty-buffer:jar:4.0.30.Final
  native.maven_jar(
      name = "io_netty_netty_common",
      artifact = "io.netty:netty-common:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "77c59fa687e86e8bc24506e308aaf7e96ffc575e",
  )


  # org.apache.spark:spark-network-common_2.10:jar:1.6.1
  # org.apache.spark:spark-core_2.10:jar:1.6.1 got requested version
  # org.apache.spark:spark-network-shuffle_2.10:jar:1.6.1 got requested version
  # org.apache.spark:spark-unsafe_2.10:jar:1.6.1 got requested version
  native.maven_jar(
      name = "org_spark_project_spark_unused",
      artifact = "org.spark-project.spark:unused:1.0.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "205fe37a2fade6ce6dfcf8eff57ed21a4a1c22af",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_reflections_reflections",
      artifact = "org.reflections:reflections:0.9.9-RC1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "b78b545f452a6b7d4fab2641dd0b0147a0f4fd5e",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_spark_spark_unsafe_2_10",
      artifact = "org.apache.spark:spark-unsafe_2.10:1.6.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "08439f2146820e370030af8faf2378e335c99f85",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_locationtech_jts_jts_core",
      artifact = "org.locationtech.jts:jts-core:1.15.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "705981b7e25d05a76a3654e597dab6ba423eb79e",
  )


  # org.antlr:ST4:jar:4.0.8 got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20 got requested version
  # org.janusgraph:janusgraph-es:jar:0.3.0 wanted version 3.2
  # org.antlr:antlr:jar:3.5.2
  native.maven_jar(
      name = "org_antlr_antlr_runtime",
      artifact = "org.antlr:antlr-runtime:3.5.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "cd9cd41361c155f3af0f653009dcecb08d8b4afd",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_thinkaurelius_thrift_thrift_server",
      artifact = "com.thinkaurelius.thrift:thrift-server:0.3.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e8182774da1b1dde3704f450837c79997b5d7025",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_noggit_noggit",
      artifact = "org.noggit:noggit:0.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "fa94a59c44b39ee710f3c9451750119e432326c0",
  )


  # org.codehaus.jettison:jettison:bundle:1.2
  native.maven_jar(
      name = "stax_stax_api",
      artifact = "stax:stax-api:1.0.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "49c100caf72d658aca8e58bd74a4ba90fa2b0d70",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_ntriples",
      artifact = "org.openrdf.sesame:sesame-rio-ntriples:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ea534875690e22c1cb245f51f94cdae4ae44a910",
  )


  # io.netty:netty-codec-socks:jar:4.0.30.Final wanted version 4.0.30.Final
  # io.netty:netty-codec-http:jar:4.0.30.Final wanted version 4.0.30.Final
  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "io_netty_netty_handler",
      artifact = "io.netty:netty-handler:4.0.47.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "caf9f8c2bd54938c01548afa5082b6341ddd30a8",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1 got requested version
  # com.twitter:chill_2.10:jar:0.5.0
  native.maven_jar(
      name = "com_twitter_chill_java",
      artifact = "com.twitter:chill-java:0.5.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "4b72c40509612ae221db6a2c2f733bd3e365ee5f",
  )


  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.6.6
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_paranamer",
      artifact = "com.fasterxml.jackson.module:jackson-module-paranamer:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3d8f5dcc16254665da6415f1bae79065c5b5d81a",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "org_antlr_antlr",
      artifact = "org.antlr:antlr:3.5.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c4a65c950bfc3e7d04309c515b2177c00baf7764",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_trix",
      artifact = "org.openrdf.sesame:sesame-rio-trix:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0dfa3cf1b813ad17c457f334220ddaa169ac6c08",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_locationtech_spatial4j_spatial4j",
      artifact = "org.locationtech.spatial4j:spatial4j:0.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "faa8ba85d503da4ab872d17ba8c00da0098ab2f2",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_sharegov_mjson",
      artifact = "org.sharegov:mjson:1.4.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "62b9e87ffd8189092962ca90f067be359221ece0",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_janusgraph_janusgraph_hadoop",
      artifact = "org.janusgraph:janusgraph-hadoop:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "4108c9e9f322e9cd5f54571899c634d065a2c460",
  )


  # net.java.dev.jets3t:jets3t:jar:0.9.1
  native.maven_jar(
      name = "com_jamesmurty_utils_java_xmlbuilder",
      artifact = "com.jamesmurty.utils:java-xmlbuilder:1.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "4a6507aa7da3d7db5dd23eedf1185649384c3bc3",
  )


  # com.addthis.metrics:reporter-config:jar:2.1.0 got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "com_yammer_metrics_metrics_core",
      artifact = "com.yammer.metrics:metrics-core:2.2.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f82c035cfa786d3cbec362c38c22a5f5b1bc8724",
  )


  # org.tachyonproject:tachyon-client:jar:0.8.2
  native.maven_jar(
      name = "org_tachyonproject_tachyon_underfs_s3",
      artifact = "org.tachyonproject:tachyon-underfs-s3:0.8.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "019843af3d333f5257500e3faab4b65553b94b0b",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "com_github_jnr_jnr_x86asm",
      artifact = "com.github.jnr:jnr-x86asm:1.0.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "006936bbd6c5b235665d87bd450f5e13b52d4b48",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  # io.grpc:grpc-protobuf:jar:1.8.0 got requested version
  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT got requested version
  # io.grpc:grpc-protobuf-lite:jar:1.8.0 got requested version
  # io.grpc:grpc-netty:jar:1.8.0 got requested version
  # io.grpc:grpc-stub:jar:1.8.0 got requested version
  native.maven_jar(
      name = "io_grpc_grpc_core",
      artifact = "io.grpc:grpc-core:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2e9753ad0cd44942937bd5c260a0f1e80ce7a463",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_xbean_xbean_asm5_shaded",
      artifact = "org.apache.xbean:xbean-asm5-shaded:4.4",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "a413bb5a8571d4c86a47e8a0272ba7ab0d1a17f5",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_json4s_json4s_jackson_2_10",
      artifact = "org.json4s:json4s-jackson_2.10:3.2.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d52c4a71a17bc806593d0e868e1d32dca4257da9",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  native.maven_jar(
      name = "org_apache_tinkerpop_gremlin_shaded",
      artifact = "org.apache.tinkerpop:gremlin-shaded:3.3.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "046eafd935485ef326d91039a1e8f36677070efe",
  )


  # org.apache.httpcomponents:httpclient:jar:4.5.1
  # org.apache.thrift:libthrift:pom:0.9.1 wanted version 4.2.4
  # net.java.dev.jets3t:jets3t:jar:0.9.1 wanted version 4.3.1
  # org.elasticsearch.client:elasticsearch-rest-client:jar:6.0.1 wanted version 4.4.5
  native.maven_jar(
      name = "org_apache_httpcomponents_httpcore",
      artifact = "org.apache.httpcomponents:httpcore:4.4.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "e876a79d561e5c6207b78d347e198c8c4531a5e5",
  )


  # io.netty:netty-handler-proxy:jar:4.0.30.Final
  native.maven_jar(
      name = "io_netty_netty_codec_socks",
      artifact = "io.netty:netty-codec-socks:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "5067ced8b14c26097797fa80dc8febe284789bb8",
  )


  # io.netty:netty-transport:jar:4.0.30.Final
  native.maven_jar(
      name = "io_netty_netty_buffer",
      artifact = "io.netty:netty-buffer:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1fba127ac617ac4eb9d53a5297d26877c41ce4f8",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_netty",
      artifact = "io.grpc:grpc-netty:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "085334a9da3902c15d87e8d879c147f9ee424145",
  )


  # pom.xml got requested version
  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # ai.grakn:core.dashboard:jar:1.4.0-SNAPSHOT got requested version
  # ch.qos.logback:logback-classic:jar:1.2.3 got requested version
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.1.2
  native.maven_jar(
      name = "ch_qos_logback_logback_core",
      artifact = "ch.qos.logback:logback-core:1.2.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "864344400c3d4d92dfeb0a305dc87d953677c03c",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_trig",
      artifact = "org.openrdf.sesame:sesame-rio-trig:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "cda5c3c3a6426b1b790d4b41c86c875709f87f93",
  )


  # io.opencensus:opencensus-contrib-grpc-metrics:jar:0.8.0 got requested version
  # io.grpc:grpc-core:jar:1.8.0
  native.maven_jar(
      name = "io_opencensus_opencensus_api",
      artifact = "io.opencensus:opencensus-api:0.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f921cd399ff9a3084370969dca74ccea510ff91f",
  )


  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-languages:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-api:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-datatypes:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-ntriples:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_model",
      artifact = "org.openrdf.sesame:sesame-model:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "daa96d0fa343ecaabe39c6c394922a428580ab2b",
  )


  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-languages:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-datatypes:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-ntriples:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_api",
      artifact = "org.openrdf.sesame:sesame-rio-api:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "5e6959e041bdf10a236d888dba83a236153edcd8",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "com_datastax_cassandra_cassandra_driver_core",
      artifact = "com.datastax.cassandra:cassandra-driver-core:3.3.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "5b47e34195d97b3a78f2b88e716665ca6f2e180f",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # org.janusgraph:janusgraph-core:jar:0.3.0 got requested version
  native.maven_jar(
      name = "org_apache_tinkerpop_tinkergraph_gremlin",
      artifact = "org.apache.tinkerpop:tinkergraph-gremlin:3.3.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "07aee25058dc82e3f2851a5d761f4c70252f1484",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 2.5.2
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_clearspring_analytics_stream",
      artifact = "com.clearspring.analytics:stream:2.7.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "9998f8cf87d329fef226405f8d519638cfe1431d",
  )


  # io.grpc:grpc-netty:jar:1.8.0
  native.maven_jar(
      name = "io_netty_netty_codec_http2",
      artifact = "io.netty:netty-codec-http2:4.1.16.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "45c27cddac120a4fcda8f699659e59389f7b9736",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 got requested version
  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_turtle",
      artifact = "org.openrdf.sesame:sesame-rio-turtle:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "34cfd1cfa76b927ce8dfb7fe817d891e64a9b08d",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "com_codahale_metrics_metrics_ganglia",
      artifact = "com.codahale.metrics:metrics-ganglia:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1c574e1b154dab30302668f4a474bd8ecf89e5b5",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_jcl_over_slf4j",
      artifact = "org.slf4j:jcl-over-slf4j:1.7.20",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "722d5b58cb054a835605fe4d12ae163513f48d2e",
  )


  # io.grpc:grpc-core:jar:1.8.0
  native.maven_jar(
      name = "io_grpc_grpc_context",
      artifact = "io.grpc:grpc-context:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7fe8214b8d1141afadbe0bcad751df2b8fe2e078",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_n3",
      artifact = "org.openrdf.sesame:sesame-rio-n3:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "feb8c7abd4c10230872ef93ab9f08185edf52b5c",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_netflix_hystrix_hystrix_core",
      artifact = "com.netflix.hystrix:hystrix-core:1.5.12",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "75379b6671fcaa9cec33035df684a68ec7741ca6",
  )


  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 2.4
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3 got requested version
  # commons-configuration:commons-configuration:jar:1.10
  native.maven_jar(
      name = "commons_lang_commons_lang",
      artifact = "commons-lang:commons-lang:2.6",
      repository = "http://central.maven.org/maven2/",
      sha1 = "0ce1edb914c94ebc388f086c6827e8bdeec71ac2",
  )


  # org.apache.spark:spark-network-shuffle_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_fusesource_leveldbjni_leveldbjni_all",
      artifact = "org.fusesource.leveldbjni:leveldbjni-all:1.8",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "707350a2eeb1fa2ed77a32ddb3893ed308e941db",
  )


  # com.google.code.findbugs:annotations:jar:3.0.1
  # io.grpc:grpc-core:jar:1.8.0 wanted version 3.0.0
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 3.0.0
  # com.github.rholder:guava-retrying:jar:2.0.0 wanted version 2.0.2
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT wanted version 2.0.2
  # com.google.instrumentation:instrumentation-api:jar:0.4.3 wanted version 3.0.0
  native.maven_jar(
      name = "com_google_code_findbugs_jsr305",
      artifact = "com.google.code.findbugs:jsr305:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "f7be08ec23c21485b9b5a1cf1654c2ec8c58168d",
  )


  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.11.8
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2
  native.maven_jar(
      name = "org_scala_lang_scala_reflect",
      artifact = "org.scala-lang:scala-reflect:2.10.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3259f3df0f166f017ef5b2d385445808398c316c",
  )


  # io.netty:netty-handler-proxy:jar:4.0.30.Final
  # io.netty:netty-codec:jar:4.0.30.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_transport",
      artifact = "io.netty:netty-transport:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "12a620e1caeab0fbbd103f877cd81dff0a41c475",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  # com.github.jnr:jnr-ffi:jar:2.0.7 got requested version
  native.maven_jar(
      name = "com_github_jnr_jffi",
      artifact = "com.github.jnr:jffi:1.2.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d58fdb2283456bc3f049bfbef40b592fa1aaa975",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "org_ow2_asm_asm_util",
      artifact = "org.ow2.asm:asm-util:5.0.3",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "1512e5571325854b05fb1efce1db75fcced54389",
  )


  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-ntriples:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_languages",
      artifact = "org.openrdf.sesame:sesame-rio-languages:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ec2869ff97f57fde9728e7c8075fdf15e7c0c592",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_log4j_over_slf4j",
      artifact = "org.slf4j:log4j-over-slf4j:1.7.20",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c8fee323e89bf28cb8b85d6ed1a29c5b8f52a829",
  )


  # org.scala-lang:scalap:jar:2.10.0
  native.maven_jar(
      name = "org_scala_lang_scala_compiler",
      artifact = "org.scala-lang:scala-compiler:2.10.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0fec8066cd2b4f8dc7ff7ba7a8e0a792939d9f9a",
  )


  # org.tachyonproject:tachyon-client:jar:0.8.2
  native.maven_jar(
      name = "org_tachyonproject_tachyon_underfs_hdfs",
      artifact = "org.tachyonproject:tachyon-underfs-hdfs:0.8.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "36f23a23bae1c007110c1e2560fe563cbccb75f6",
  )


  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3 wanted version 2.6
  # com.fasterxml.jackson.module:jackson-module-paranamer:bundle:2.9.2
  native.maven_jar(
      name = "com_thoughtworks_paranamer_paranamer",
      artifact = "com.thoughtworks.paranamer:paranamer:2.8",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "619eba74c19ccf1da8ebec97a2d7f8ba05773dd6",
  )


  # io.opencensus:opencensus-contrib-grpc-metrics:jar:0.8.0 got requested version
  # io.grpc:grpc-core:jar:1.8.0
  native.maven_jar(
      name = "com_google_errorprone_error_prone_annotations",
      artifact = "com.google.errorprone:error_prone_annotations:2.0.19",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "c3754a0bdd545b00ddc26884f9e7624f8b6a14de",
  )


  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0
  native.maven_jar(
      name = "org_codehaus_jettison_jettison",
      artifact = "org.codehaus.jettison:jettison:1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "0765a6181653f4b05c18c7a9e8f5c1f8269bf9b2",
  )


  # com.esotericsoftware.kryo:kryo:bundle:2.21
  native.maven_jar(
      name = "org_objenesis_objenesis",
      artifact = "org.objenesis:objenesis:1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "bfcb0539a071a4c5a30690388903ac48c0667f2a",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # org.janusgraph:janusgraph-es:jar:0.3.0 got requested version
  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 got requested version
  native.maven_jar(
      name = "org_janusgraph_janusgraph_core",
      artifact = "org.janusgraph:janusgraph-core:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "9283efee3be01c5866bb812da126658ee0a87d29",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_janusgraph_janusgraph_hbase",
      artifact = "org.janusgraph:janusgraph-hbase:0.3.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "b4e073ac94fc15b38a31d6d0604c6dc89af0366f",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_11",
      artifact = "com.fasterxml.jackson.module:jackson-module-scala_2.11:2.6.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "122197f0251ee46ff5ce5c3f00abc0cac4fa546a",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_10",
      artifact = "com.fasterxml.jackson.module:jackson-module-scala_2.10:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3ea410e61cc498b892f0ee4ea2118ef4d0beb35c",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 3.2
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_commons_commons_math3",
      artifact = "org.apache.commons:commons-math3:3.4.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "3ac44a8664228384bc68437264cf7c4cf112f579",
  )


  # org.apache.spark:spark-unsafe_2.10:jar:1.6.1 got requested version
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_twitter_chill_2_10",
      artifact = "com.twitter:chill_2.10:0.5.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "763dd57f6496e1ed9eb5728ac49d054226d21267",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 got requested version
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.2.0
  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "net_jpountz_lz4_lz4",
      artifact = "net.jpountz.lz4:lz4:1.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "792d5e592f6f3f0c1a3337cd0ac84309b544f8f4",
  )


  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-turtle:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-rdfxml:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trix:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  # org.openrdf.sesame:sesame-rio-ntriples:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_datatypes",
      artifact = "org.openrdf.sesame:sesame-rio-datatypes:2.7.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "9a476a95c03eea5877b197b72c0624d7cecf171f",
  )


  # com.twitter:chill_2.10:jar:0.5.0 got requested version
  # com.twitter:chill-java:jar:0.5.0
  native.maven_jar(
      name = "com_esotericsoftware_kryo_kryo",
      artifact = "com.esotericsoftware.kryo:kryo:2.21",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "09a4e69cff8d225729656f7e97e40893b23bffef",
  )


  # com.netflix.hystrix:hystrix-core:jar:1.5.12
  native.maven_jar(
      name = "com_netflix_archaius_archaius_core",
      artifact = "com.netflix.archaius:archaius-core:0.4.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "69e956ddf4543c989461352a214f32a014abd64a",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_eclipse_jetty_orbit_javax_servlet",
      artifact = "org.eclipse.jetty.orbit:javax.servlet:3.0.0.v201112011016",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "0aaaa85845fb5c59da00193f06b8e5278d8bf3f8",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "com_typesafe_akka_akka_remote_2_10",
      artifact = "com.typesafe.akka:akka-remote_2.10:2.3.11",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "09aed4ee314df6f54079bb4befdd77050e567b9d",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "org_ow2_asm_asm_commons",
      artifact = "org.ow2.asm:asm-commons:5.0.3",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "a7111830132c7f87d08fe48cb0ca07630f8cb91c",
  )


  # org.reflections:reflections:jar:0.9.9-RC1
  native.maven_jar(
      name = "org_javassist_javassist",
      artifact = "org.javassist:javassist:3.16.1-GA",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "315891b371395271977af518d4db5cee1a0bc9bf",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "oro_oro",
      artifact = "oro:oro:2.0.8",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "5592374f834645c4ae250f4c9fbb314c9369d698",
  )


  # com.esotericsoftware.kryo:kryo:bundle:2.21
  native.maven_jar(
      name = "com_esotericsoftware_minlog_minlog",
      artifact = "com.esotericsoftware.minlog:minlog:1.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "59bfcd171d82f9981a5e242b9e840191f650e209",
  )


  # com.netflix.astyanax:astyanax-core:jar:3.8.0
  native.maven_jar(
      name = "joda_time_joda_time",
      artifact = "joda-time:joda-time:1.6.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7a0525fe460ef5b99ea3152e6d2c0e4f24f04c51",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_netty_netty_tcnative_boringssl_static",
      artifact = "io.netty:netty-tcnative-boringssl-static:2.0.14.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "d75ef93513d0cf4e61cacfa83e1f6f97ae0cdddf",
  )


  # com.github.jnr:jnr-posix:jar:3.0.27
  native.maven_jar(
      name = "com_github_jnr_jnr_constants",
      artifact = "com.github.jnr:jnr-constants:0.9.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "6894684e17a84cd500836e82b5e6c674b4d4dda6",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_protobuf",
      artifact = "io.grpc:grpc-protobuf:1.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "749848c287ef01b110a8fe464965e5cff730a7ac",
  )


  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0
  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 got requested version
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 got requested version
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_cassandra",
      artifact = "com.netflix.astyanax:astyanax-cassandra:3.8.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "8b9704784b34229faace4aa986d08b80dba6aac6",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "net_razorvine_pyrolite",
      artifact = "net.razorvine:pyrolite:4.9",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ed38335f0609a1a623921096724726d9b823c938",
  )


  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0 wanted version 2.0.12
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 wanted version 2.0.12
  # org.apache.cassandra:cassandra-all:jar:2.1.20
  native.maven_jar(
      name = "org_apache_cassandra_cassandra_thrift",
      artifact = "org.apache.cassandra:cassandra-thrift:2.1.20",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "2e24ed193c46ddba5cc1be24e51290ed3dcd28ab",
  )


  # com.codahale.metrics:metrics-ganglia:bundle:3.0.1
  native.maven_jar(
      name = "info_ganglia_gmetric4j_gmetric4j",
      artifact = "info.ganglia.gmetric4j:gmetric4j:1.0.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "badb330453496c7a2465148903b3bd2a49462307",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_github_rholder_guava_retrying",
      artifact = "com.github.rholder:guava-retrying:2.0.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "974bc0a04a11cc4806f7c20a34703bd23c34e7f4",
  )


  # org.apache.spark:spark-core_2.10:jar:1.6.1
  native.maven_jar(
      name = "org_apache_mesos_mesos",
      artifact = "org.apache.mesos:mesos:0.21.1",
      repository = "https://repo.spring.io/libs-release/",
      sha1 = "a960be5884b3ef034b09f85592db361de8fe937a",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  # org.janusgraph:janusgraph-core:jar:0.3.0 got requested version
  native.maven_jar(
      name = "commons_collections_commons_collections",
      artifact = "commons-collections:commons-collections:3.2.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "8ad72fe39fa8c91eaaf12aadb21e0c3661fe26d5",
  )


  # com.googlecode.json-simple:json-simple:bundle:1.1.1 wanted version 4.10
  # org.sharegov:mjson:bundle:1.4.0
  # com.thinkaurelius.thrift:thrift-server:jar:0.3.7 wanted version 4.8.1
  native.maven_jar(
      name = "junit_junit",
      artifact = "junit:junit:4.12",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2973d150c0dc1fefe998f834810d68f278ea58ec",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # com.netflix.hystrix:hystrix-core:jar:1.5.12 wanted version 1.2.0
  native.maven_jar(
      name = "io_reactivex_rxjava",
      artifact = "io.reactivex:rxjava:1.3.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "bcc561a84883a9f3f84e209662c4765a0c3dc691",
  )


  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3 wanted version 1.1.3
  # org.elasticsearch.client:elasticsearch-rest-client:jar:6.0.1 wanted version 1.1.3
  # org.apache.httpcomponents:httpclient:jar:4.5.1
  native.maven_jar(
      name = "commons_logging_commons_logging",
      artifact = "commons-logging:commons-logging:1.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "4bfc12adfe4842bf07b657f0369c4cb522955686",
  )


  # io.netty:netty-handler-proxy:jar:4.0.30.Final
  native.maven_jar(
      name = "io_netty_netty_codec_http",
      artifact = "io.netty:netty-codec-http:4.0.30.Final",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2e4a8527dca92f914077d7fd488ccf0a94969fd3",
  )


  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_dropwizard_metrics_metrics_core",
      artifact = "io.dropwizard.metrics:metrics-core:3.2.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "169e950d2a9b83fff3abfb4b843029d81375229a",
  )


  # com.jcabi:jcabi-manifests:jar:1.1
  native.maven_jar(
      name = "com_jcabi_jcabi_log",
      artifact = "com.jcabi:jcabi-log:0.14",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "819a57348f2448f01d74f8a317dab61d6a90cac2",
  )


  # org.json4s:json4s-core_2.10:jar:3.2.10
  native.maven_jar(
      name = "org_scala_lang_scalap",
      artifact = "org.scala-lang:scalap:2.10.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "ab42ae21d1fd7311b367fe3d7f33343f2e4bff6b",
  )


  # com.typesafe.akka:akka-slf4j_2.10:jar:2.3.11 got requested version
  # com.typesafe.akka:akka-remote_2.10:jar:2.3.11
  native.maven_jar(
      name = "com_typesafe_akka_akka_actor_2_10",
      artifact = "com.typesafe.akka:akka-actor_2.10:2.3.11",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "cb99662d461833308c760d1abca07bc389084259",
  )


  # com.typesafe.akka:akka-remote_2.10:jar:2.3.11
  native.maven_jar(
      name = "org_uncommons_maths_uncommons_maths",
      artifact = "org.uncommons.maths:uncommons-maths:1.2.2a",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e4e84f879d4160be9508aa3904516ec30c39e815",
  )


  # org.json4s:json4s-jackson_2.10:jar:3.2.10
  native.maven_jar(
      name = "org_json4s_json4s_core_2_10",
      artifact = "org.json4s:json4s-core_2.10:3.2.10",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "dad833a33b231556e6254ab609eedfe8364793d3",
  )


  # org.apache.thrift:libthrift:pom:0.9.1 wanted version 3.1
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 3.1
  # org.apache.cassandra:cassandra-thrift:jar:2.1.20 wanted version 3.1
  # org.apache.tinkerpop:tinkergraph-gremlin:jar:3.3.3
  native.maven_jar(
      name = "org_apache_commons_commons_lang3",
      artifact = "org.apache.commons:commons-lang3:3.3.1",
      repository = "http://central.maven.org/maven2/",
      sha1 = "6738a2da2202ce360f0af90aba005c1e05a2c4cd",
  )


  # io.grpc:grpc-protobuf:jar:1.8.0
  native.maven_jar(
      name = "com_google_api_grpc_proto_google_common_protos",
      artifact = "com.google.api.grpc:proto-google-common-protos:0.1.9",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3760f6a6e13c8ab070aa629876cdd183614ee877",
  )


  # io.grpc:grpc-protobuf:jar:1.8.0 got requested version
  # ai.grakn:core.console:jar:1.4.0-SNAPSHOT got requested version
  # org.reflections:reflections:jar:0.9.9-RC1 wanted version 11.0.2
  # io.grpc:grpc-protobuf-lite:jar:1.8.0 got requested version
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # org.tachyonproject:tachyon-underfs-local:jar:0.8.2 wanted version 14.0.1
  # com.github.rholder:guava-retrying:jar:2.0.0 wanted version 26.0-jre
  # ai.grakn:client-java:jar:1.4.0-SNAPSHOT got requested version
  # io.opencensus:opencensus-api:jar:0.8.0 got requested version
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 18.0
  # org.tachyonproject:tachyon-underfs-hdfs:jar:0.8.2 wanted version 14.0.1
  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 16.0
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 wanted version 15.0
  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 15.0
  # org.tachyonproject:tachyon-client:jar:0.8.2 wanted version 14.0.1
  # org.tachyonproject:tachyon-underfs-s3:jar:0.8.2 wanted version 14.0.1
  # io.grpc:grpc-core:jar:1.8.0 got requested version
  # com.google.instrumentation:instrumentation-api:jar:0.4.3 got requested version
  # com.google.protobuf:protobuf-java-util:bundle:3.4.0 got requested version
  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2 got requested version
  native.maven_jar(
      name = "com_google_guava_guava",
      artifact = "com.google.guava:guava:19.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "6ce200f6b23222af3d8abb6b6459e6c44f4bb0e9",
  )


  # org.apache.tinkerpop:gremlin-core:jar:3.3.3
  native.maven_jar(
      name = "net_objecthunter_exp4j",
      artifact = "net.objecthunter:exp4j:0.4.8",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "cf1cfc0f958077d86ac7452c7e36d944689b2ec4",
  )


  # org.apache.cassandra:cassandra-all:jar:2.1.20 wanted version 1.9.2
  # org.apache.tinkerpop:hadoop-gremlin:jar:3.3.3
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 wanted version 1.9.2
  # net.java.dev.jets3t:jets3t:jar:0.9.1 wanted version 1.9.12
  native.maven_jar(
      name = "org_codehaus_jackson_jackson_mapper_asl",
      artifact = "org.codehaus.jackson:jackson-mapper-asl:1.9.13",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1ee2f2bed0e5dd29d1cb155a166e6f8d50bbddb7",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "com_codahale_metrics_metrics_graphite",
      artifact = "com.codahale.metrics:metrics-graphite:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2389e1501d8b9b1ab3b2cd8da16afef56430ba15",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT wanted version 3.2.5
  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 got requested version
  # org.apache.tinkerpop:spark-gremlin:jar:3.3.3
  native.maven_jar(
      name = "org_apache_tinkerpop_hadoop_gremlin",
      artifact = "org.apache.tinkerpop:hadoop-gremlin:3.3.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "bd3fdc75707ddf01a2faf92a7dba66d15e1b1f14",
  )




def generated_java_libraries():
  native.java_library(
      name = "com_esotericsoftware_reflectasm_reflectasm",
      visibility = ["//visibility:public"],
      exports = ["@com_esotericsoftware_reflectasm_reflectasm//jar"],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_recipes",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_recipes//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":com_netflix_astyanax_astyanax_cassandra",
          ":com_netflix_astyanax_astyanax_core",
      ],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_es",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_es//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_classic",
          ":commons_codec_commons_codec",
          ":commons_logging_commons_logging",
          ":org_antlr_antlr_runtime",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_elasticsearch_client_elasticsearch_rest_client",
          ":org_janusgraph_janusgraph_core",
      ],
  )


  native.java_library(
      name = "com_google_auto_value_auto_value",
      visibility = ["//visibility:public"],
      exports = ["@com_google_auto_value_auto_value//jar"],
  )


  native.java_library(
      name = "net_iharder_base64",
      visibility = ["//visibility:public"],
      exports = ["@net_iharder_base64//jar"],
  )


  native.java_library(
      name = "org_apache_spark_spark_core_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_spark_spark_core_2_10//jar"],
      runtime_deps = [
          ":com_clearspring_analytics_stream",
          ":com_esotericsoftware_kryo_kryo",
          ":com_esotericsoftware_minlog_minlog",
          ":com_esotericsoftware_reflectasm_reflectasm",
          ":com_google_guava_guava",
          ":com_google_protobuf_protobuf_java",
          ":com_jamesmurty_utils_java_xmlbuilder",
          ":com_ning_compress_lzf",
          ":com_sun_jersey_jersey_core",
          ":com_twitter_chill_2_10",
          ":com_twitter_chill_java",
          ":com_typesafe_akka_akka_actor_2_10",
          ":com_typesafe_akka_akka_remote_2_10",
          ":com_typesafe_akka_akka_slf4j_2_10",
          ":com_typesafe_config",
          ":commons_io_commons_io",
          ":commons_net_commons_net",
          ":io_dropwizard_metrics_metrics_graphite",
          ":io_dropwizard_metrics_metrics_json",
          ":io_dropwizard_metrics_metrics_jvm",
          ":io_netty_netty",
          ":io_netty_netty_all",
          ":javax_mail_mail",
          ":mx4j_mx4j",
          ":net_iharder_base64",
          ":net_java_dev_jets3t_jets3t",
          ":net_jpountz_lz4_lz4",
          ":net_razorvine_pyrolite",
          ":net_sf_py4j_py4j",
          ":org_apache_commons_commons_math3",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_mesos_mesos",
          ":org_apache_spark_spark_network_common_2_10",
          ":org_apache_spark_spark_network_shuffle_2_10",
          ":org_apache_spark_spark_unsafe_2_10",
          ":org_apache_xbean_xbean_asm5_shaded",
          ":org_bouncycastle_bcprov_jdk15",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_eclipse_jetty_orbit_javax_servlet",
          ":org_fusesource_leveldbjni_leveldbjni_all",
          ":org_json4s_json4s_ast_2_10",
          ":org_json4s_json4s_core_2_10",
          ":org_json4s_json4s_jackson_2_10",
          ":org_objenesis_objenesis",
          ":org_roaringbitmap_RoaringBitmap",
          ":org_scala_lang_scala_compiler",
          ":org_scala_lang_scalap",
          ":org_slf4j_jul_to_slf4j",
          ":org_spark_project_spark_unused",
          ":org_tachyonproject_tachyon_client",
          ":org_tachyonproject_tachyon_underfs_hdfs",
          ":org_tachyonproject_tachyon_underfs_local",
          ":org_tachyonproject_tachyon_underfs_s3",
          ":org_uncommons_maths_uncommons_maths",
          ":oro_oro",
      ],
  )


  native.java_library(
      name = "com_google_protobuf_protobuf_java_util",
      visibility = ["//visibility:public"],
      exports = ["@com_google_protobuf_protobuf_java_util//jar"],
      runtime_deps = [
          ":com_google_code_gson_gson",
          ":com_google_guava_guava",
          ":com_google_protobuf_protobuf_java",
      ],
  )


  native.java_library(
      name = "org_apache_spark_spark_network_shuffle_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_spark_spark_network_shuffle_2_10//jar"],
      runtime_deps = [
          ":org_apache_spark_spark_network_common_2_10",
          ":org_fusesource_leveldbjni_leveldbjni_all",
          ":org_spark_project_spark_unused",
      ],
  )


  native.java_library(
      name = "org_apache_cassandra_cassandra_all",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_cassandra_cassandra_all//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_core",
          ":com_addthis_metrics_reporter_config",
          ":com_clearspring_analytics_stream",
          ":com_github_jbellis_jamm",
          ":com_google_guava_guava",
          ":com_googlecode_concurrentlinkedhashmap_concurrentlinkedhashmap_lru",
          ":com_googlecode_json_simple_json_simple",
          ":com_lmax_disruptor",
          ":com_ning_compress_lzf",
          ":com_thinkaurelius_thrift_thrift_server",
          ":com_yammer_metrics_metrics_core",
          ":commons_cli_commons_cli",
          ":commons_codec_commons_codec",
          ":io_netty_netty_all",
          ":junit_junit",
          ":net_java_dev_jna_jna",
          ":net_jpountz_lz4_lz4",
          ":net_sf_supercsv_super_csv",
          ":org_antlr_ST4",
          ":org_antlr_antlr",
          ":org_antlr_antlr_runtime",
          ":org_apache_cassandra_cassandra_thrift",
          ":org_apache_commons_commons_lang3",
          ":org_apache_commons_commons_math3",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_thrift_libthrift",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_xerial_snappy_snappy_java",
          ":org_yaml_snakeyaml",
      ],
  )


  native.java_library(
      name = "org_apache_spark_spark_network_common_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_spark_spark_network_common_2_10//jar"],
      runtime_deps = [
          ":io_netty_netty_all",
          ":org_spark_project_spark_unused",
      ],
  )


  native.java_library(
      name = "net_java_dev_jets3t_jets3t",
      visibility = ["//visibility:public"],
      exports = ["@net_java_dev_jets3t_jets3t//jar"],
      runtime_deps = [
          ":com_jamesmurty_utils_java_xmlbuilder",
          ":javax_mail_mail",
          ":mx4j_mx4j",
          ":net_iharder_base64",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_bouncycastle_bcprov_jdk15",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
      ],
  )


  native.java_library(
      name = "javax_validation_validation_api",
      visibility = ["//visibility:public"],
      exports = ["@javax_validation_validation_api//jar"],
  )


  native.java_library(
      name = "com_boundary_high_scale_lib",
      visibility = ["//visibility:public"],
      exports = ["@com_boundary_high_scale_lib//jar"],
  )


  native.java_library(
      name = "org_codehaus_jackson_jackson_core_asl",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jackson_jackson_core_asl//jar"],
  )


  native.java_library(
      name = "org_ow2_asm_asm_tree",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm_tree//jar"],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_databind",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_databind//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
      ],
  )


  native.java_library(
      name = "org_antlr_ST4",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_ST4//jar"],
      runtime_deps = [
          ":org_antlr_antlr_runtime",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_core",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_core//jar"],
  )


  native.java_library(
      name = "org_slf4j_slf4j_simple",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_slf4j_simple//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "ch_qos_logback_logback_classic",
      visibility = ["//visibility:public"],
      exports = ["@ch_qos_logback_logback_classic//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_core",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "com_lmax_disruptor",
      visibility = ["//visibility:public"],
      exports = ["@com_lmax_disruptor//jar"],
  )


  native.java_library(
      name = "com_google_protobuf_protobuf_java",
      visibility = ["//visibility:public"],
      exports = ["@com_google_protobuf_protobuf_java//jar"],
  )


  native.java_library(
      name = "jline_jline",
      visibility = ["//visibility:public"],
      exports = ["@jline_jline//jar"],
  )


  native.java_library(
      name = "org_hdrhistogram_HdrHistogram",
      visibility = ["//visibility:public"],
      exports = ["@org_hdrhistogram_HdrHistogram//jar"],
  )


  native.java_library(
      name = "com_ning_compress_lzf",
      visibility = ["//visibility:public"],
      exports = ["@com_ning_compress_lzf//jar"],
  )


  native.java_library(
      name = "net_sf_py4j_py4j",
      visibility = ["//visibility:public"],
      exports = ["@net_sf_py4j_py4j//jar"],
  )


  native.java_library(
      name = "com_codahale_metrics_metrics_core",
      visibility = ["//visibility:public"],
      exports = ["@com_codahale_metrics_metrics_core//jar"],
  )


  native.java_library(
      name = "com_addthis_metrics_reporter_config",
      visibility = ["//visibility:public"],
      exports = ["@com_addthis_metrics_reporter_config//jar"],
      runtime_deps = [
          ":com_yammer_metrics_metrics_core",
          ":org_yaml_snakeyaml",
      ],
  )


  native.java_library(
      name = "org_javatuples_javatuples",
      visibility = ["//visibility:public"],
      exports = ["@org_javatuples_javatuples//jar"],
  )


  native.java_library(
      name = "commons_net_commons_net",
      visibility = ["//visibility:public"],
      exports = ["@commons_net_commons_net//jar"],
  )


  native.java_library(
      name = "io_netty_netty_codec",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec//jar"],
      runtime_deps = [
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "org_roaringbitmap_RoaringBitmap",
      visibility = ["//visibility:public"],
      exports = ["@org_roaringbitmap_RoaringBitmap//jar"],
  )


  native.java_library(
      name = "org_tachyonproject_tachyon_client",
      visibility = ["//visibility:public"],
      exports = ["@org_tachyonproject_tachyon_client//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":commons_io_commons_io",
          ":org_tachyonproject_tachyon_underfs_hdfs",
          ":org_tachyonproject_tachyon_underfs_local",
          ":org_tachyonproject_tachyon_underfs_s3",
      ],
  )


  native.java_library(
      name = "org_slf4j_slf4j_api",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_slf4j_api//jar"],
  )


  native.java_library(
      name = "com_typesafe_config",
      visibility = ["//visibility:public"],
      exports = ["@com_typesafe_config//jar"],
  )


  native.java_library(
      name = "org_apache_tinkerpop_gremlin_core",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_tinkerpop_gremlin_core//jar"],
      runtime_deps = [
          ":com_carrotsearch_hppc",
          ":com_jcabi_jcabi_log",
          ":com_jcabi_jcabi_manifests",
          ":com_squareup_javapoet",
          ":commons_collections_commons_collections",
          ":commons_configuration_commons_configuration",
          ":commons_lang_commons_lang",
          ":net_objecthunter_exp4j",
          ":org_apache_tinkerpop_gremlin_shaded",
          ":org_javatuples_javatuples",
          ":org_slf4j_slf4j_api",
          ":org_yaml_snakeyaml",
      ],
  )


  native.java_library(
      name = "com_squareup_javapoet",
      visibility = ["//visibility:public"],
      exports = ["@com_squareup_javapoet//jar"],
  )


  native.java_library(
      name = "com_google_code_gson_gson",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_gson_gson//jar"],
  )


  native.java_library(
      name = "com_jcabi_jcabi_manifests",
      visibility = ["//visibility:public"],
      exports = ["@com_jcabi_jcabi_manifests//jar"],
      runtime_deps = [
          ":com_jcabi_jcabi_log",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_annotations//jar"],
  )


  native.java_library(
      name = "org_json4s_json4s_ast_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_json4s_json4s_ast_2_10//jar"],
  )


  native.java_library(
      name = "org_apache_httpcomponents_httpclient",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_httpcomponents_httpclient//jar"],
      runtime_deps = [
          ":commons_codec_commons_codec",
          ":commons_logging_commons_logging",
          ":org_apache_httpcomponents_httpcore",
      ],
  )


  native.java_library(
      name = "javax_ws_rs_jsr311_api",
      visibility = ["//visibility:public"],
      exports = ["@javax_ws_rs_jsr311_api//jar"],
  )


  native.java_library(
      name = "io_netty_netty_all",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_all//jar"],
  )


  native.java_library(
      name = "javax_mail_mail",
      visibility = ["//visibility:public"],
      exports = ["@javax_mail_mail//jar"],
  )


  native.java_library(
      name = "net_sf_supercsv_super_csv",
      visibility = ["//visibility:public"],
      exports = ["@net_sf_supercsv_super_csv//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_protobuf_lite",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_protobuf_lite//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":io_grpc_grpc_core",
      ],
  )


  native.java_library(
      name = "io_netty_netty_handler_proxy",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_handler_proxy//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_codec_http",
          ":io_netty_netty_codec_socks",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "io_netty_netty",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty//jar"],
  )


  native.java_library(
      name = "io_dropwizard_metrics_metrics_jvm",
      visibility = ["//visibility:public"],
      exports = ["@io_dropwizard_metrics_metrics_jvm//jar"],
  )


  native.java_library(
      name = "org_slf4j_jul_to_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_jul_to_slf4j//jar"],
  )


  native.java_library(
      name = "commons_configuration_commons_configuration",
      visibility = ["//visibility:public"],
      exports = ["@commons_configuration_commons_configuration//jar"],
      runtime_deps = [
          ":commons_lang_commons_lang",
      ],
  )


  native.java_library(
      name = "org_apache_commons_commons_text",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_commons_commons_text//jar"],
  )


  native.java_library(
      name = "org_ow2_asm_asm",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm//jar"],
  )


  native.java_library(
      name = "com_googlecode_concurrentlinkedhashmap_concurrentlinkedhashmap_lru",
      visibility = ["//visibility:public"],
      exports = ["@com_googlecode_concurrentlinkedhashmap_concurrentlinkedhashmap_lru//jar"],
  )


  native.java_library(
      name = "io_opencensus_opencensus_contrib_grpc_metrics",
      visibility = ["//visibility:public"],
      exports = ["@io_opencensus_opencensus_contrib_grpc_metrics//jar"],
      runtime_deps = [
          ":com_google_errorprone_error_prone_annotations",
          ":io_opencensus_opencensus_api",
      ],
  )


  native.java_library(
      name = "org_xerial_snappy_snappy_java",
      visibility = ["//visibility:public"],
      exports = ["@org_xerial_snappy_snappy_java//jar"],
  )


  native.java_library(
      name = "net_java_dev_jna_jna",
      visibility = ["//visibility:public"],
      exports = ["@net_java_dev_jna_jna//jar"],
  )


  native.java_library(
      name = "org_hamcrest_hamcrest_core",
      visibility = ["//visibility:public"],
      exports = ["@org_hamcrest_hamcrest_core//jar"],
  )


  native.java_library(
      name = "com_typesafe_akka_akka_slf4j_2_10",
      visibility = ["//visibility:public"],
      exports = ["@com_typesafe_akka_akka_slf4j_2_10//jar"],
      runtime_deps = [
          ":com_typesafe_akka_akka_actor_2_10",
      ],
  )


  native.java_library(
      name = "org_tachyonproject_tachyon_underfs_local",
      visibility = ["//visibility:public"],
      exports = ["@org_tachyonproject_tachyon_underfs_local//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "com_github_jnr_jnr_posix",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_posix//jar"],
      runtime_deps = [
          ":com_github_jnr_jnr_constants",
          ":com_github_jnr_jnr_ffi",
      ],
  )


  native.java_library(
      name = "io_dropwizard_metrics_metrics_json",
      visibility = ["//visibility:public"],
      exports = ["@io_dropwizard_metrics_metrics_json//jar"],
  )


  native.java_library(
      name = "com_google_instrumentation_instrumentation_api",
      visibility = ["//visibility:public"],
      exports = ["@com_google_instrumentation_instrumentation_api//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "com_googlecode_json_simple_json_simple",
      visibility = ["//visibility:public"],
      exports = ["@com_googlecode_json_simple_json_simple//jar"],
      runtime_deps = [
          ":junit_junit",
      ],
  )


  native.java_library(
      name = "org_scala_lang_scala_library",
      visibility = ["//visibility:public"],
      exports = ["@org_scala_lang_scala_library//jar"],
  )


  native.java_library(
      name = "io_dropwizard_metrics_metrics_graphite",
      visibility = ["//visibility:public"],
      exports = ["@io_dropwizard_metrics_metrics_graphite//jar"],
  )


  native.java_library(
      name = "mx4j_mx4j",
      visibility = ["//visibility:public"],
      exports = ["@mx4j_mx4j//jar"],
  )


  native.java_library(
      name = "com_eaio_uuid_uuid",
      visibility = ["//visibility:public"],
      exports = ["@com_eaio_uuid_uuid//jar"],
  )


  native.java_library(
      name = "org_abego_treelayout_org_abego_treelayout_core",
      visibility = ["//visibility:public"],
      exports = ["@org_abego_treelayout_org_abego_treelayout_core//jar"],
  )


  native.java_library(
      name = "dom4j_dom4j",
      visibility = ["//visibility:public"],
      exports = ["@dom4j_dom4j//jar"],
      runtime_deps = [
          ":xml_apis_xml_apis",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_stub",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_stub//jar"],
      runtime_deps = [
          ":io_grpc_grpc_core",
      ],
  )


  native.java_library(
      name = "commons_cli_commons_cli",
      visibility = ["//visibility:public"],
      exports = ["@commons_cli_commons_cli//jar"],
  )


  native.java_library(
      name = "org_apache_hadoop_hadoop_client",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_hadoop_hadoop_client//jar"],
  )


  native.java_library(
      name = "org_apache_thrift_libthrift",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_thrift_libthrift//jar"],
      runtime_deps = [
          ":org_apache_commons_commons_lang3",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
      ],
  )


  native.java_library(
      name = "com_github_jbellis_jamm",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jbellis_jamm//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jnr_ffi",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_ffi//jar"],
      runtime_deps = [
          ":com_github_jnr_jffi",
          ":com_github_jnr_jnr_x86asm",
          ":org_ow2_asm_asm",
          ":org_ow2_asm_asm_analysis",
          ":org_ow2_asm_asm_commons",
          ":org_ow2_asm_asm_tree",
          ":org_ow2_asm_asm_util",
      ],
  )


  native.java_library(
      name = "org_apache_tinkerpop_spark_gremlin",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_tinkerpop_spark_gremlin//jar"],
      runtime_deps = [
          ":com_clearspring_analytics_stream",
          ":com_esotericsoftware_kryo_kryo",
          ":com_esotericsoftware_minlog_minlog",
          ":com_esotericsoftware_reflectasm_reflectasm",
          ":com_google_guava_guava",
          ":com_google_protobuf_protobuf_java",
          ":com_jamesmurty_utils_java_xmlbuilder",
          ":com_ning_compress_lzf",
          ":com_sun_jersey_jersey_core",
          ":com_thoughtworks_paranamer_paranamer",
          ":com_twitter_chill_2_10",
          ":com_twitter_chill_java",
          ":com_typesafe_akka_akka_actor_2_10",
          ":com_typesafe_akka_akka_remote_2_10",
          ":com_typesafe_akka_akka_slf4j_2_10",
          ":com_typesafe_config",
          ":commons_codec_commons_codec",
          ":commons_io_commons_io",
          ":commons_lang_commons_lang",
          ":commons_logging_commons_logging",
          ":commons_net_commons_net",
          ":io_dropwizard_metrics_metrics_graphite",
          ":io_dropwizard_metrics_metrics_json",
          ":io_dropwizard_metrics_metrics_jvm",
          ":io_netty_netty",
          ":io_netty_netty_all",
          ":javax_mail_mail",
          ":mx4j_mx4j",
          ":net_iharder_base64",
          ":net_java_dev_jets3t_jets3t",
          ":net_jpountz_lz4_lz4",
          ":net_razorvine_pyrolite",
          ":net_sf_py4j_py4j",
          ":org_apache_commons_commons_math3",
          ":org_apache_hadoop_hadoop_client",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_mesos_mesos",
          ":org_apache_spark_spark_core_2_10",
          ":org_apache_spark_spark_network_common_2_10",
          ":org_apache_spark_spark_network_shuffle_2_10",
          ":org_apache_spark_spark_unsafe_2_10",
          ":org_apache_tinkerpop_gremlin_core",
          ":org_apache_tinkerpop_hadoop_gremlin",
          ":org_apache_xbean_xbean_asm5_shaded",
          ":org_bouncycastle_bcprov_jdk15",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_eclipse_jetty_orbit_javax_servlet",
          ":org_fusesource_leveldbjni_leveldbjni_all",
          ":org_json4s_json4s_ast_2_10",
          ":org_json4s_json4s_core_2_10",
          ":org_json4s_json4s_jackson_2_10",
          ":org_objenesis_objenesis",
          ":org_roaringbitmap_RoaringBitmap",
          ":org_scala_lang_scala_compiler",
          ":org_scala_lang_scala_library",
          ":org_scala_lang_scalap",
          ":org_slf4j_jul_to_slf4j",
          ":org_spark_project_spark_unused",
          ":org_tachyonproject_tachyon_client",
          ":org_tachyonproject_tachyon_underfs_hdfs",
          ":org_tachyonproject_tachyon_underfs_local",
          ":org_tachyonproject_tachyon_underfs_s3",
          ":org_uncommons_maths_uncommons_maths",
          ":org_xerial_snappy_snappy_java",
          ":oro_oro",
      ],
  )


  native.java_library(
      name = "org_antlr_antlr4_runtime",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_antlr4_runtime//jar"],
      runtime_deps = [
          ":org_abego_treelayout_org_abego_treelayout_core",
      ],
  )


  native.java_library(
      name = "commons_pool_commons_pool",
      visibility = ["//visibility:public"],
      exports = ["@commons_pool_commons_pool//jar"],
  )


  native.java_library(
      name = "net_jodah_failsafe",
      visibility = ["//visibility:public"],
      exports = ["@net_jodah_failsafe//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_rdfxml",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_rdfxml//jar"],
      runtime_deps = [
          ":commons_io_commons_io",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "org_elasticsearch_client_elasticsearch_rest_client",
      visibility = ["//visibility:public"],
      exports = ["@org_elasticsearch_client_elasticsearch_rest_client//jar"],
      runtime_deps = [
          ":commons_codec_commons_codec",
          ":commons_logging_commons_logging",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
      ],
  )


  native.java_library(
      name = "org_ow2_asm_asm_analysis",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm_analysis//jar"],
  )


  native.java_library(
      name = "org_yaml_snakeyaml",
      visibility = ["//visibility:public"],
      exports = ["@org_yaml_snakeyaml//jar"],
  )


  native.java_library(
      name = "com_github_stephenc_high_scale_lib_high_scale_lib",
      visibility = ["//visibility:public"],
      exports = ["@com_github_stephenc_high_scale_lib_high_scale_lib//jar"],
  )


  native.java_library(
      name = "com_google_code_findbugs_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_findbugs_annotations//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
      ],
  )


  native.java_library(
      name = "com_carrotsearch_hppc",
      visibility = ["//visibility:public"],
      exports = ["@com_carrotsearch_hppc//jar"],
  )


  native.java_library(
      name = "commons_io_commons_io",
      visibility = ["//visibility:public"],
      exports = ["@commons_io_commons_io//jar"],
  )


  native.java_library(
      name = "commons_codec_commons_codec",
      visibility = ["//visibility:public"],
      exports = ["@commons_codec_commons_codec//jar"],
  )


  native.java_library(
      name = "com_sun_jersey_jersey_core",
      visibility = ["//visibility:public"],
      exports = ["@com_sun_jersey_jersey_core//jar"],
  )


  native.java_library(
      name = "xml_apis_xml_apis",
      visibility = ["//visibility:public"],
      exports = ["@xml_apis_xml_apis//jar"],
  )


  native.java_library(
      name = "org_bouncycastle_bcprov_jdk15",
      visibility = ["//visibility:public"],
      exports = ["@org_bouncycastle_bcprov_jdk15//jar"],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_thrift",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_thrift//jar"],
      runtime_deps = [
          ":com_netflix_astyanax_astyanax_cassandra",
          ":com_netflix_astyanax_astyanax_core",
          ":commons_codec_commons_codec",
          ":org_apache_cassandra_cassandra_all",
          ":org_apache_cassandra_cassandra_thrift",
          ":org_apache_thrift_libthrift",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_xerial_snappy_snappy_java",
          ":stax_stax_api",
      ],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_cassandra",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_cassandra//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_core",
          ":com_addthis_metrics_reporter_config",
          ":com_clearspring_analytics_stream",
          ":com_eaio_uuid_uuid",
          ":com_github_jbellis_jamm",
          ":com_github_stephenc_high_scale_lib_high_scale_lib",
          ":com_google_guava_guava",
          ":com_googlecode_concurrentlinkedhashmap_concurrentlinkedhashmap_lru",
          ":com_googlecode_json_simple_json_simple",
          ":com_lmax_disruptor",
          ":com_netflix_astyanax_astyanax_cassandra",
          ":com_netflix_astyanax_astyanax_core",
          ":com_netflix_astyanax_astyanax_recipes",
          ":com_netflix_astyanax_astyanax_thrift",
          ":com_ning_compress_lzf",
          ":com_thinkaurelius_thrift_thrift_server",
          ":com_yammer_metrics_metrics_core",
          ":commons_cli_commons_cli",
          ":commons_codec_commons_codec",
          ":commons_lang_commons_lang",
          ":commons_pool_commons_pool",
          ":io_netty_netty_all",
          ":javax_validation_validation_api",
          ":joda_time_joda_time",
          ":junit_junit",
          ":net_java_dev_jna_jna",
          ":net_jpountz_lz4_lz4",
          ":net_sf_supercsv_super_csv",
          ":org_antlr_ST4",
          ":org_antlr_antlr",
          ":org_antlr_antlr_runtime",
          ":org_apache_cassandra_cassandra_all",
          ":org_apache_cassandra_cassandra_thrift",
          ":org_apache_commons_commons_lang3",
          ":org_apache_commons_commons_math3",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_thrift_libthrift",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_janusgraph_janusgraph_core",
          ":org_xerial_snappy_snappy_java",
          ":org_yaml_snakeyaml",
          ":stax_stax_api",
      ],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_core",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_core//jar"],
      runtime_deps = [
          ":com_eaio_uuid_uuid",
          ":com_github_stephenc_high_scale_lib_high_scale_lib",
          ":com_google_guava_guava",
          ":commons_lang_commons_lang",
          ":joda_time_joda_time",
          ":org_apache_cassandra_cassandra_all",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_util",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_util//jar"],
  )


  native.java_library(
      name = "io_netty_netty_common",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_common//jar"],
  )


  native.java_library(
      name = "org_spark_project_spark_unused",
      visibility = ["//visibility:public"],
      exports = ["@org_spark_project_spark_unused//jar"],
  )


  native.java_library(
      name = "org_reflections_reflections",
      visibility = ["//visibility:public"],
      exports = ["@org_reflections_reflections//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":dom4j_dom4j",
          ":org_javassist_javassist",
          ":xml_apis_xml_apis",
      ],
  )


  native.java_library(
      name = "org_apache_spark_spark_unsafe_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_spark_spark_unsafe_2_10//jar"],
      runtime_deps = [
          ":com_twitter_chill_2_10",
          ":org_spark_project_spark_unused",
      ],
  )


  native.java_library(
      name = "org_locationtech_jts_jts_core",
      visibility = ["//visibility:public"],
      exports = ["@org_locationtech_jts_jts_core//jar"],
  )


  native.java_library(
      name = "org_antlr_antlr_runtime",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_antlr_runtime//jar"],
  )


  native.java_library(
      name = "com_thinkaurelius_thrift_thrift_server",
      visibility = ["//visibility:public"],
      exports = ["@com_thinkaurelius_thrift_thrift_server//jar"],
      runtime_deps = [
          ":com_lmax_disruptor",
          ":junit_junit",
          ":org_apache_commons_commons_lang3",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_thrift_libthrift",
      ],
  )


  native.java_library(
      name = "org_noggit_noggit",
      visibility = ["//visibility:public"],
      exports = ["@org_noggit_noggit//jar"],
  )


  native.java_library(
      name = "stax_stax_api",
      visibility = ["//visibility:public"],
      exports = ["@stax_stax_api//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_ntriples",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_ntriples//jar"],
      runtime_deps = [
          ":commons_io_commons_io",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
      ],
  )


  native.java_library(
      name = "io_netty_netty_handler",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_handler//jar"],
  )


  native.java_library(
      name = "com_twitter_chill_java",
      visibility = ["//visibility:public"],
      exports = ["@com_twitter_chill_java//jar"],
      runtime_deps = [
          ":com_esotericsoftware_kryo_kryo",
          ":com_esotericsoftware_minlog_minlog",
          ":com_esotericsoftware_reflectasm_reflectasm",
          ":org_objenesis_objenesis",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_module_jackson_module_paranamer",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_module_jackson_module_paranamer//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_databind",
          ":com_thoughtworks_paranamer_paranamer",
      ],
  )


  native.java_library(
      name = "org_antlr_antlr",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_antlr//jar"],
      runtime_deps = [
          ":org_antlr_ST4",
          ":org_antlr_antlr_runtime",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_trix",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_trix//jar"],
      runtime_deps = [
          ":commons_io_commons_io",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "org_locationtech_spatial4j_spatial4j",
      visibility = ["//visibility:public"],
      exports = ["@org_locationtech_spatial4j_spatial4j//jar"],
  )


  native.java_library(
      name = "org_sharegov_mjson",
      visibility = ["//visibility:public"],
      exports = ["@org_sharegov_mjson//jar"],
      runtime_deps = [
          ":junit_junit",
          ":org_hamcrest_hamcrest_core",
      ],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_hadoop",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_hadoop//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_classic",
          ":com_datastax_cassandra_cassandra_driver_core",
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
          ":com_fasterxml_jackson_core_jackson_databind",
          ":com_fasterxml_jackson_module_jackson_module_paranamer",
          ":com_fasterxml_jackson_module_jackson_module_scala_2_11",
          ":com_github_jnr_jffi",
          ":com_github_jnr_jnr_constants",
          ":com_github_jnr_jnr_ffi",
          ":com_github_jnr_jnr_posix",
          ":com_github_jnr_jnr_x86asm",
          ":com_google_guava_guava",
          ":commons_codec_commons_codec",
          ":commons_io_commons_io",
          ":commons_logging_commons_logging",
          ":io_netty_netty_handler",
          ":org_antlr_antlr_runtime",
          ":org_apache_httpcomponents_httpclient",
          ":org_apache_httpcomponents_httpcore",
          ":org_apache_tinkerpop_hadoop_gremlin",
          ":org_apache_tinkerpop_spark_gremlin",
          ":org_apache_tinkerpop_tinkergraph_gremlin",
          ":org_elasticsearch_client_elasticsearch_rest_client",
          ":org_janusgraph_janusgraph_core",
          ":org_janusgraph_janusgraph_es",
          ":org_janusgraph_janusgraph_hbase",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_rio_n3",
          ":org_openrdf_sesame_sesame_rio_ntriples",
          ":org_openrdf_sesame_sesame_rio_rdfxml",
          ":org_openrdf_sesame_sesame_rio_trig",
          ":org_openrdf_sesame_sesame_rio_trix",
          ":org_openrdf_sesame_sesame_rio_turtle",
          ":org_openrdf_sesame_sesame_util",
          ":org_ow2_asm_asm",
          ":org_ow2_asm_asm_analysis",
          ":org_ow2_asm_asm_commons",
          ":org_ow2_asm_asm_tree",
          ":org_ow2_asm_asm_util",
          ":org_scala_lang_scala_reflect",
      ],
  )


  native.java_library(
      name = "com_jamesmurty_utils_java_xmlbuilder",
      visibility = ["//visibility:public"],
      exports = ["@com_jamesmurty_utils_java_xmlbuilder//jar"],
      runtime_deps = [
          ":net_iharder_base64",
      ],
  )


  native.java_library(
      name = "com_yammer_metrics_metrics_core",
      visibility = ["//visibility:public"],
      exports = ["@com_yammer_metrics_metrics_core//jar"],
  )


  native.java_library(
      name = "org_tachyonproject_tachyon_underfs_s3",
      visibility = ["//visibility:public"],
      exports = ["@org_tachyonproject_tachyon_underfs_s3//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "com_github_jnr_jnr_x86asm",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_x86asm//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_core",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_core//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
          ":com_google_errorprone_error_prone_annotations",
          ":com_google_guava_guava",
          ":com_google_instrumentation_instrumentation_api",
          ":io_grpc_grpc_context",
          ":io_opencensus_opencensus_api",
          ":io_opencensus_opencensus_contrib_grpc_metrics",
      ],
  )


  native.java_library(
      name = "org_apache_xbean_xbean_asm5_shaded",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_xbean_xbean_asm5_shaded//jar"],
  )


  native.java_library(
      name = "org_json4s_json4s_jackson_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_json4s_json4s_jackson_2_10//jar"],
      runtime_deps = [
          ":org_json4s_json4s_ast_2_10",
          ":org_json4s_json4s_core_2_10",
          ":org_scala_lang_scala_compiler",
          ":org_scala_lang_scalap",
      ],
  )


  native.java_library(
      name = "org_apache_tinkerpop_gremlin_shaded",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_tinkerpop_gremlin_shaded//jar"],
  )


  native.java_library(
      name = "org_apache_httpcomponents_httpcore",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_httpcomponents_httpcore//jar"],
  )


  native.java_library(
      name = "io_netty_netty_codec_socks",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_socks//jar"],
      runtime_deps = [
          ":io_netty_netty_codec",
          ":io_netty_netty_handler",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "io_netty_netty_buffer",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_buffer//jar"],
      runtime_deps = [
          ":io_netty_netty_common",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_netty",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_netty//jar"],
      runtime_deps = [
          ":io_grpc_grpc_core",
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_codec_http",
          ":io_netty_netty_codec_http2",
          ":io_netty_netty_codec_socks",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_handler_proxy",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "ch_qos_logback_logback_core",
      visibility = ["//visibility:public"],
      exports = ["@ch_qos_logback_logback_core//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_trig",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_trig//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_rio_turtle",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "io_opencensus_opencensus_api",
      visibility = ["//visibility:public"],
      exports = ["@io_opencensus_opencensus_api//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_model",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_model//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_api",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_api//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "com_datastax_cassandra_cassandra_driver_core",
      visibility = ["//visibility:public"],
      exports = ["@com_datastax_cassandra_cassandra_driver_core//jar"],
      runtime_deps = [
          ":com_github_jnr_jffi",
          ":com_github_jnr_jnr_constants",
          ":com_github_jnr_jnr_ffi",
          ":com_github_jnr_jnr_posix",
          ":com_github_jnr_jnr_x86asm",
          ":com_google_guava_guava",
          ":io_netty_netty_handler",
          ":org_ow2_asm_asm",
          ":org_ow2_asm_asm_analysis",
          ":org_ow2_asm_asm_commons",
          ":org_ow2_asm_asm_tree",
          ":org_ow2_asm_asm_util",
      ],
  )


  native.java_library(
      name = "org_apache_tinkerpop_tinkergraph_gremlin",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_tinkerpop_tinkergraph_gremlin//jar"],
      runtime_deps = [
          ":org_apache_commons_commons_lang3",
          ":org_apache_tinkerpop_gremlin_core",
      ],
  )


  native.java_library(
      name = "com_clearspring_analytics_stream",
      visibility = ["//visibility:public"],
      exports = ["@com_clearspring_analytics_stream//jar"],
  )


  native.java_library(
      name = "io_netty_netty_codec_http2",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_http2//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_turtle",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_turtle//jar"],
      runtime_deps = [
          ":commons_io_commons_io",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "com_codahale_metrics_metrics_ganglia",
      visibility = ["//visibility:public"],
      exports = ["@com_codahale_metrics_metrics_ganglia//jar"],
      runtime_deps = [
          ":com_codahale_metrics_metrics_core",
          ":info_ganglia_gmetric4j_gmetric4j",
      ],
  )


  native.java_library(
      name = "org_slf4j_jcl_over_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_jcl_over_slf4j//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_context",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_context//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_n3",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_n3//jar"],
      runtime_deps = [
          ":commons_io_commons_io",
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_rio_datatypes",
          ":org_openrdf_sesame_sesame_rio_languages",
          ":org_openrdf_sesame_sesame_rio_turtle",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "com_netflix_hystrix_hystrix_core",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_hystrix_hystrix_core//jar"],
      runtime_deps = [
          ":com_netflix_archaius_archaius_core",
          ":commons_configuration_commons_configuration",
          ":io_reactivex_rxjava",
          ":org_hdrhistogram_HdrHistogram",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "commons_lang_commons_lang",
      visibility = ["//visibility:public"],
      exports = ["@commons_lang_commons_lang//jar"],
  )


  native.java_library(
      name = "org_fusesource_leveldbjni_leveldbjni_all",
      visibility = ["//visibility:public"],
      exports = ["@org_fusesource_leveldbjni_leveldbjni_all//jar"],
  )


  native.java_library(
      name = "com_google_code_findbugs_jsr305",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_findbugs_jsr305//jar"],
  )


  native.java_library(
      name = "org_scala_lang_scala_reflect",
      visibility = ["//visibility:public"],
      exports = ["@org_scala_lang_scala_reflect//jar"],
      runtime_deps = [
          ":org_scala_lang_scala_library",
      ],
  )


  native.java_library(
      name = "io_netty_netty_transport",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_transport//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_common",
      ],
  )


  native.java_library(
      name = "com_github_jnr_jffi",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jffi//jar"],
  )


  native.java_library(
      name = "org_ow2_asm_asm_util",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm_util//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_languages",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_languages//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "org_slf4j_log4j_over_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_log4j_over_slf4j//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "org_scala_lang_scala_compiler",
      visibility = ["//visibility:public"],
      exports = ["@org_scala_lang_scala_compiler//jar"],
  )


  native.java_library(
      name = "org_tachyonproject_tachyon_underfs_hdfs",
      visibility = ["//visibility:public"],
      exports = ["@org_tachyonproject_tachyon_underfs_hdfs//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "com_thoughtworks_paranamer_paranamer",
      visibility = ["//visibility:public"],
      exports = ["@com_thoughtworks_paranamer_paranamer//jar"],
  )


  native.java_library(
      name = "com_google_errorprone_error_prone_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_google_errorprone_error_prone_annotations//jar"],
  )


  native.java_library(
      name = "org_codehaus_jettison_jettison",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jettison_jettison//jar"],
      runtime_deps = [
          ":stax_stax_api",
      ],
  )


  native.java_library(
      name = "org_objenesis_objenesis",
      visibility = ["//visibility:public"],
      exports = ["@org_objenesis_objenesis//jar"],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_core",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_core//jar"],
      runtime_deps = [
          ":com_carrotsearch_hppc",
          ":com_codahale_metrics_metrics_core",
          ":com_codahale_metrics_metrics_ganglia",
          ":com_codahale_metrics_metrics_graphite",
          ":com_github_stephenc_high_scale_lib_high_scale_lib",
          ":com_google_code_findbugs_jsr305",
          ":com_google_guava_guava",
          ":commons_codec_commons_codec",
          ":commons_collections_commons_collections",
          ":commons_configuration_commons_configuration",
          ":commons_io_commons_io",
          ":dom4j_dom4j",
          ":info_ganglia_gmetric4j_gmetric4j",
          ":org_apache_commons_commons_text",
          ":org_apache_tinkerpop_gremlin_core",
          ":org_apache_tinkerpop_tinkergraph_gremlin",
          ":org_javassist_javassist",
          ":org_locationtech_jts_jts_core",
          ":org_locationtech_spatial4j_spatial4j",
          ":org_noggit_noggit",
          ":org_reflections_reflections",
          ":xml_apis_xml_apis",
      ],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_hbase",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_hbase//jar"],
  )


  native.java_library(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_11",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_module_jackson_module_scala_2_11//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
          ":com_fasterxml_jackson_core_jackson_databind",
          ":com_fasterxml_jackson_module_jackson_module_paranamer",
          ":org_scala_lang_scala_reflect",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_10",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_module_jackson_module_scala_2_10//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
          ":com_fasterxml_jackson_core_jackson_databind",
          ":com_fasterxml_jackson_module_jackson_module_paranamer",
          ":com_thoughtworks_paranamer_paranamer",
          ":org_scala_lang_scala_library",
          ":org_scala_lang_scala_reflect",
      ],
  )


  native.java_library(
      name = "org_apache_commons_commons_math3",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_commons_commons_math3//jar"],
  )


  native.java_library(
      name = "com_twitter_chill_2_10",
      visibility = ["//visibility:public"],
      exports = ["@com_twitter_chill_2_10//jar"],
      runtime_deps = [
          ":com_esotericsoftware_kryo_kryo",
          ":com_esotericsoftware_minlog_minlog",
          ":com_esotericsoftware_reflectasm_reflectasm",
          ":com_twitter_chill_java",
          ":org_objenesis_objenesis",
      ],
  )


  native.java_library(
      name = "net_jpountz_lz4_lz4",
      visibility = ["//visibility:public"],
      exports = ["@net_jpountz_lz4_lz4//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_datatypes",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_datatypes//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_rio_api",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "com_esotericsoftware_kryo_kryo",
      visibility = ["//visibility:public"],
      exports = ["@com_esotericsoftware_kryo_kryo//jar"],
      runtime_deps = [
          ":com_esotericsoftware_minlog_minlog",
          ":com_esotericsoftware_reflectasm_reflectasm",
          ":org_objenesis_objenesis",
      ],
  )


  native.java_library(
      name = "com_netflix_archaius_archaius_core",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_archaius_archaius_core//jar"],
      runtime_deps = [
          ":commons_configuration_commons_configuration",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "org_eclipse_jetty_orbit_javax_servlet",
      visibility = ["//visibility:public"],
      exports = ["@org_eclipse_jetty_orbit_javax_servlet//jar"],
  )


  native.java_library(
      name = "com_typesafe_akka_akka_remote_2_10",
      visibility = ["//visibility:public"],
      exports = ["@com_typesafe_akka_akka_remote_2_10//jar"],
      runtime_deps = [
          ":com_google_protobuf_protobuf_java",
          ":com_typesafe_akka_akka_actor_2_10",
          ":com_typesafe_config",
          ":io_netty_netty",
          ":org_uncommons_maths_uncommons_maths",
      ],
  )


  native.java_library(
      name = "org_ow2_asm_asm_commons",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm_commons//jar"],
  )


  native.java_library(
      name = "org_javassist_javassist",
      visibility = ["//visibility:public"],
      exports = ["@org_javassist_javassist//jar"],
  )


  native.java_library(
      name = "oro_oro",
      visibility = ["//visibility:public"],
      exports = ["@oro_oro//jar"],
  )


  native.java_library(
      name = "com_esotericsoftware_minlog_minlog",
      visibility = ["//visibility:public"],
      exports = ["@com_esotericsoftware_minlog_minlog//jar"],
  )


  native.java_library(
      name = "joda_time_joda_time",
      visibility = ["//visibility:public"],
      exports = ["@joda_time_joda_time//jar"],
  )


  native.java_library(
      name = "io_netty_netty_tcnative_boringssl_static",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_tcnative_boringssl_static//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jnr_constants",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_constants//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_protobuf",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_protobuf//jar"],
      runtime_deps = [
          ":com_google_api_grpc_proto_google_common_protos",
          ":com_google_code_gson_gson",
          ":com_google_guava_guava",
          ":com_google_protobuf_protobuf_java",
          ":com_google_protobuf_protobuf_java_util",
          ":io_grpc_grpc_core",
          ":io_grpc_grpc_protobuf_lite",
      ],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_cassandra",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_cassandra//jar"],
      runtime_deps = [
          ":com_netflix_astyanax_astyanax_core",
          ":commons_codec_commons_codec",
          ":org_apache_cassandra_cassandra_all",
          ":org_apache_cassandra_cassandra_thrift",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_xerial_snappy_snappy_java",
          ":stax_stax_api",
      ],
  )


  native.java_library(
      name = "net_razorvine_pyrolite",
      visibility = ["//visibility:public"],
      exports = ["@net_razorvine_pyrolite//jar"],
  )


  native.java_library(
      name = "org_apache_cassandra_cassandra_thrift",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_cassandra_cassandra_thrift//jar"],
      runtime_deps = [
          ":org_apache_commons_commons_lang3",
          ":org_apache_thrift_libthrift",
      ],
  )


  native.java_library(
      name = "info_ganglia_gmetric4j_gmetric4j",
      visibility = ["//visibility:public"],
      exports = ["@info_ganglia_gmetric4j_gmetric4j//jar"],
  )


  native.java_library(
      name = "com_github_rholder_guava_retrying",
      visibility = ["//visibility:public"],
      exports = ["@com_github_rholder_guava_retrying//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
          ":com_google_guava_guava",
      ],
  )


  native.java_library(
      name = "org_apache_mesos_mesos",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_mesos_mesos//jar"],
  )


  native.java_library(
      name = "commons_collections_commons_collections",
      visibility = ["//visibility:public"],
      exports = ["@commons_collections_commons_collections//jar"],
  )


  native.java_library(
      name = "junit_junit",
      visibility = ["//visibility:public"],
      exports = ["@junit_junit//jar"],
      runtime_deps = [
          ":org_hamcrest_hamcrest_core",
      ],
  )


  native.java_library(
      name = "io_reactivex_rxjava",
      visibility = ["//visibility:public"],
      exports = ["@io_reactivex_rxjava//jar"],
  )


  native.java_library(
      name = "commons_logging_commons_logging",
      visibility = ["//visibility:public"],
      exports = ["@commons_logging_commons_logging//jar"],
  )


  native.java_library(
      name = "io_netty_netty_codec_http",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_http//jar"],
      runtime_deps = [
          ":io_netty_netty_codec",
          ":io_netty_netty_handler",
      ],
  )


  native.java_library(
      name = "io_dropwizard_metrics_metrics_core",
      visibility = ["//visibility:public"],
      exports = ["@io_dropwizard_metrics_metrics_core//jar"],
  )


  native.java_library(
      name = "com_jcabi_jcabi_log",
      visibility = ["//visibility:public"],
      exports = ["@com_jcabi_jcabi_log//jar"],
  )


  native.java_library(
      name = "org_scala_lang_scalap",
      visibility = ["//visibility:public"],
      exports = ["@org_scala_lang_scalap//jar"],
      runtime_deps = [
          ":org_scala_lang_scala_compiler",
      ],
  )


  native.java_library(
      name = "com_typesafe_akka_akka_actor_2_10",
      visibility = ["//visibility:public"],
      exports = ["@com_typesafe_akka_akka_actor_2_10//jar"],
      runtime_deps = [
          ":com_typesafe_config",
      ],
  )


  native.java_library(
      name = "org_uncommons_maths_uncommons_maths",
      visibility = ["//visibility:public"],
      exports = ["@org_uncommons_maths_uncommons_maths//jar"],
  )


  native.java_library(
      name = "org_json4s_json4s_core_2_10",
      visibility = ["//visibility:public"],
      exports = ["@org_json4s_json4s_core_2_10//jar"],
      runtime_deps = [
          ":org_json4s_json4s_ast_2_10",
          ":org_scala_lang_scala_compiler",
          ":org_scala_lang_scalap",
      ],
  )


  native.java_library(
      name = "org_apache_commons_commons_lang3",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_commons_commons_lang3//jar"],
  )


  native.java_library(
      name = "com_google_api_grpc_proto_google_common_protos",
      visibility = ["//visibility:public"],
      exports = ["@com_google_api_grpc_proto_google_common_protos//jar"],
  )


  native.java_library(
      name = "com_google_guava_guava",
      visibility = ["//visibility:public"],
      exports = ["@com_google_guava_guava//jar"],
  )


  native.java_library(
      name = "net_objecthunter_exp4j",
      visibility = ["//visibility:public"],
      exports = ["@net_objecthunter_exp4j//jar"],
  )


  native.java_library(
      name = "org_codehaus_jackson_jackson_mapper_asl",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jackson_jackson_mapper_asl//jar"],
      runtime_deps = [
          ":org_codehaus_jackson_jackson_core_asl",
      ],
  )


  native.java_library(
      name = "com_codahale_metrics_metrics_graphite",
      visibility = ["//visibility:public"],
      exports = ["@com_codahale_metrics_metrics_graphite//jar"],
      runtime_deps = [
          ":com_codahale_metrics_metrics_core",
      ],
  )


  native.java_library(
      name = "org_apache_tinkerpop_hadoop_gremlin",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_tinkerpop_hadoop_gremlin//jar"],
      runtime_deps = [
          ":commons_codec_commons_codec",
          ":commons_logging_commons_logging",
          ":org_apache_hadoop_hadoop_client",
          ":org_apache_tinkerpop_gremlin_core",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
      ],
  )
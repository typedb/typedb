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
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_code_findbugs_annotations",
      artifact = "com.google.code.findbugs:annotations:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "fc019a2216218990d64dfe756e7aa20f0069dea2",
  )


  # com.google.code.findbugs:annotations:jar:3.0.1
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 3.0.0
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


  # io.netty:netty-handler:jar:4.0.47.Final
  # io.netty:netty-codec:jar:4.0.47.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_transport",
      artifact = "io.netty:netty-transport:4.0.47.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "f5dc76021988fd1dc4e7bcb2fb0bc237b8b2098d",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_recipes",
      artifact = "com.netflix.astyanax:astyanax-recipes:3.8.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "71cf76ec6ffd6142f3e73101b9d6a4c35a67cd2f",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "com_carrotsearch_hppc",
      artifact = "com.carrotsearch:hppc:0.7.1",
      repository = "http://central.maven.org/maven2/",
      sha1 = "8b5057f74ea378c0150a1860874a3ebdcb713767",
  )


  # com.github.jnr:jnr-ffi:jar:2.0.7
  # com.github.jnr:jnr-ffi:jar:2.0.7 got requested version
  native.maven_jar(
      name = "com_github_jnr_jffi",
      artifact = "com.github.jnr:jffi:1.2.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "d58fdb2283456bc3f049bfbef40b592fa1aaa975",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_janusgraph_janusgraph_es",
      artifact = "org.janusgraph:janusgraph-es:0.3.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "c726f76022d534cb7907e36663052e15bdcbb078",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "ec2869ff97f57fde9728e7c8075fdf15e7c0c592",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_log4j_over_slf4j",
      artifact = "org.slf4j:log4j-over-slf4j:1.7.20",
      repository = "http://central.maven.org/maven2/",
      sha1 = "c8fee323e89bf28cb8b85d6ed1a29c5b8f52a829",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_auto_value_auto_value",
      artifact = "com.google.auto.value:auto-value:1.4.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "8172ebbd7970188aff304c8a420b9f17168f6f48",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_thrift",
      artifact = "com.netflix.astyanax:astyanax-thrift:3.8.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "43264bab44de99c0ff63f5b1fbe6e34591f07c1b",
  )


  # com.fasterxml.jackson.module:jackson-module-paranamer:bundle:2.9.2
  native.maven_jar(
      name = "com_thoughtworks_paranamer_paranamer",
      artifact = "com.thoughtworks.paranamer:paranamer:2.8",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "619eba74c19ccf1da8ebec97a2d7f8ba05773dd6",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_janusgraph_janusgraph_cassandra",
      artifact = "org.janusgraph:janusgraph-cassandra:0.3.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "aa2468d10049e27c2727be08ab3f3b9d579c2107",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 got requested version
  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0 got requested version
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0 got requested version
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_core",
      artifact = "com.netflix.astyanax:astyanax-core:3.8.0",
      repository = "http://central.maven.org/maven2/",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "dcab3032f0ef3f0ab7c98c84bc8119ceeaa69dc3",
  )


  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0
  native.maven_jar(
      name = "org_codehaus_jettison_jettison",
      artifact = "org.codehaus.jettison:jettison:1.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "0765a6181653f4b05c18c7a9e8f5c1f8269bf9b2",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 wanted version 1.0.5-M3
  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0
  native.maven_jar(
      name = "org_xerial_snappy_snappy_java",
      artifact = "org.xerial.snappy:snappy-java:1.0.5",
      repository = "http://central.maven.org/maven2/",
      sha1 = "10cb4550360a0ec6b80f09a5209d00b6058e82bf",
  )


  # io.netty:netty-buffer:jar:4.0.47.Final
  native.maven_jar(
      name = "io_netty_netty_common",
      artifact = "io.netty:netty-common:4.0.47.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "9e98137fd1fa1b1a08477c172c2efd112cc95616",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "b4e073ac94fc15b38a31d6d0604c6dc89af0366f",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_reflections_reflections",
      artifact = "org.reflections:reflections:0.9.9-RC1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "b78b545f452a6b7d4fab2641dd0b0147a0f4fd5e",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_11",
      artifact = "com.fasterxml.jackson.module:jackson-module-scala_2.11:2.6.6",
      repository = "http://central.maven.org/maven2/",
      sha1 = "122197f0251ee46ff5ce5c3f00abc0cac4fa546a",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_10",
      artifact = "com.fasterxml.jackson.module:jackson-module-scala_2.10:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3ea410e61cc498b892f0ee4ea2118ef4d0beb35c",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_locationtech_jts_jts_core",
      artifact = "org.locationtech.jts:jts-core:1.15.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "705981b7e25d05a76a3654e597dab6ba423eb79e",
  )


  # org.janusgraph:janusgraph-es:jar:0.3.0
  native.maven_jar(
      name = "org_antlr_antlr_runtime",
      artifact = "org.antlr:antlr-runtime:3.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "31c746001016c6226bd7356c9f87a6a084ce3715",
  )


  # junit:junit:jar:4.12
  native.maven_jar(
      name = "org_hamcrest_hamcrest_core",
      artifact = "org.hamcrest:hamcrest-core:1.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "42a25dc3219429f0e5d060061f71acb49bf010a0",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_noggit_noggit",
      artifact = "org.noggit:noggit:0.6",
      repository = "http://central.maven.org/maven2/",
      sha1 = "fa94a59c44b39ee710f3c9451750119e432326c0",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_ntriples",
      artifact = "org.openrdf.sesame:sesame-rio-ntriples:2.7.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "ea534875690e22c1cb245f51f94cdae4ae44a910",
  )


  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "io_netty_netty_handler",
      artifact = "io.netty:netty-handler:4.0.47.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "caf9f8c2bd54938c01548afa5082b6341ddd30a8",
  )


  # org.janusgraph:janusgraph-cassandra:jar:0.3.0
  native.maven_jar(
      name = "net_jpountz_lz4_lz4",
      artifact = "net.jpountz.lz4:lz4:1.3",
      repository = "http://central.maven.org/maven2/",
      sha1 = "792d5e592f6f3f0c1a3337cd0ac84309b544f8f4",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "com_boundary_high_scale_lib",
      artifact = "com.boundary:high-scale-lib:1.0.6",
      repository = "http://central.maven.org/maven2/",
      sha1 = "7b44147cb2729e1724d2d46d7b932c56b65087f0",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "9a476a95c03eea5877b197b72c0624d7cecf171f",
  )


  # com.fasterxml.jackson.module:jackson-module-scala_2.11:bundle:2.6.6 wanted version 2.6.6
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2
  native.maven_jar(
      name = "com_fasterxml_jackson_module_jackson_module_paranamer",
      artifact = "com.fasterxml.jackson.module:jackson-module-paranamer:2.9.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "3d8f5dcc16254665da6415f1bae79065c5b5d81a",
  )


  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "com_github_jnr_jnr_posix",
      artifact = "com.github.jnr:jnr-posix:3.0.27",
      repository = "http://central.maven.org/maven2/",
      sha1 = "f7441d13187d93d59656ac8f800cba3043935b59",
  )


  # org.codehaus.jackson:jackson-mapper-asl:jar:1.9.2
  native.maven_jar(
      name = "org_codehaus_jackson_jackson_core_asl",
      artifact = "org.codehaus.jackson:jackson-core-asl:1.9.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "8493982bba1727106d767034bd0d8e77bc1931a9",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_trix",
      artifact = "org.openrdf.sesame:sesame-rio-trix:2.7.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "0dfa3cf1b813ad17c457f334220ddaa169ac6c08",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "org_locationtech_spatial4j_spatial4j",
      artifact = "org.locationtech.spatial4j:spatial4j:0.7",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "faa8ba85d503da4ab872d17ba8c00da0098ab2f2",
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


  # org.reflections:reflections:jar:0.9.9-RC1
  native.maven_jar(
      name = "org_javassist_javassist",
      artifact = "org.javassist:javassist:3.16.1-GA",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "315891b371395271977af518d4db5cee1a0bc9bf",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_sharegov_mjson",
      artifact = "org.sharegov:mjson:1.4.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "62b9e87ffd8189092962ca90f067be359221ece0",
  )


  # org.scala-lang:scala-reflect:jar:2.10.6 got requested version
  # com.fasterxml.jackson.module:jackson-module-scala_2.10:bundle:2.9.2
  native.maven_jar(
      name = "org_scala_lang_scala_library",
      artifact = "org.scala-lang:scala-library:2.10.6",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "421989aa8f95a05a4f894630aad96b8c7b828732",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_janusgraph_janusgraph_hadoop",
      artifact = "org.janusgraph:janusgraph-hadoop:0.3.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "4108c9e9f322e9cd5f54571899c634d065a2c460",
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


  # com.github.jnr:jnr-ffi:jar:2.0.7
  native.maven_jar(
      name = "com_github_jnr_jnr_x86asm",
      artifact = "com.github.jnr:jnr-x86asm:1.0.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "006936bbd6c5b235665d87bd450f5e13b52d4b48",
  )


  # pom.xml got requested version
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # org.janusgraph:janusgraph-es:jar:0.3.0 wanted version 1.1.2
  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 wanted version 1.1.2
  native.maven_jar(
      name = "ch_qos_logback_logback_classic",
      artifact = "ch.qos.logback:logback-classic:1.2.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "7c4f3c474fb2c041d8028740440937705ebb473a",
  )


  # org.antlr:antlr4-runtime:jar:4.5
  native.maven_jar(
      name = "org_abego_treelayout_org_abego_treelayout_core",
      artifact = "org.abego.treelayout:org.abego.treelayout.core:1.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "e31e79cba7a5414cf18fa69f3f0a2cf9ee997b61",
  )


  # com.netflix.astyanax:astyanax-core:jar:3.8.0
  native.maven_jar(
      name = "joda_time_joda_time",
      artifact = "joda-time:joda-time:1.6.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "7a0525fe460ef5b99ea3152e6d2c0e4f24f04c51",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_netty_netty_tcnative_boringssl_static",
      artifact = "io.netty:netty-tcnative-boringssl-static:2.0.14.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "d75ef93513d0cf4e61cacfa83e1f6f97ae0cdddf",
  )


  # com.codahale.metrics:metrics-graphite:bundle:3.0.1 got requested version
  # org.janusgraph:janusgraph-core:jar:0.3.0
  # com.codahale.metrics:metrics-ganglia:bundle:3.0.1 got requested version
  native.maven_jar(
      name = "com_codahale_metrics_metrics_core",
      artifact = "com.codahale.metrics:metrics-core:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "1e98427c7f6e53363b598e2943e50903ce4f3657",
  )


  # com.github.jnr:jnr-posix:jar:3.0.27
  native.maven_jar(
      name = "com_github_jnr_jnr_constants",
      artifact = "com.github.jnr:jnr-constants:0.9.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "6894684e17a84cd500836e82b5e6c674b4d4dda6",
  )


  # com.netflix.astyanax:astyanax-thrift:jar:3.8.0
  # org.janusgraph:janusgraph-cassandra:jar:0.3.0 got requested version
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 got requested version
  native.maven_jar(
      name = "com_netflix_astyanax_astyanax_cassandra",
      artifact = "com.netflix.astyanax:astyanax-cassandra:3.8.0",
      repository = "http://central.maven.org/maven2/",
      sha1 = "8b9704784b34229faace4aa986d08b80dba6aac6",
  )


  # io.netty:netty-handler:jar:4.0.47.Final
  native.maven_jar(
      name = "io_netty_netty_codec",
      artifact = "io.netty:netty-codec:4.0.47.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "036d5841bc8da8f75d32f8af7e7c8e79bdd3e254",
  )


  # io.netty:netty-handler:jar:4.0.47.Final
  # io.netty:netty-transport:jar:4.0.47.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_buffer",
      artifact = "io.netty:netty-buffer:4.0.47.Final",
      repository = "http://central.maven.org/maven2/",
      sha1 = "b736f6a84b56c571c589318f4e585bce98d8bb77",
  )


  # pom.xml got requested version
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # ch.qos.logback:logback-classic:jar:1.2.3 got requested version
  native.maven_jar(
      name = "ch_qos_logback_logback_core",
      artifact = "ch.qos.logback:logback-core:1.2.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "864344400c3d4d92dfeb0a305dc87d953677c03c",
  )


  # com.github.jnr:jnr-posix:jar:3.0.27 got requested version
  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2
  native.maven_jar(
      name = "com_github_jnr_jnr_ffi",
      artifact = "com.github.jnr:jnr-ffi:2.0.7",
      repository = "http://central.maven.org/maven2/",
      sha1 = "f0968c5bb5a283ebda2df3604c2c1129d45196e3",
  )


  # com.codahale.metrics:metrics-ganglia:bundle:3.0.1
  native.maven_jar(
      name = "info_ganglia_gmetric4j_gmetric4j",
      artifact = "info.ganglia.gmetric4j:gmetric4j:1.0.3",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "badb330453496c7a2465148903b3bd2a49462307",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_trig",
      artifact = "org.openrdf.sesame:sesame-rio-trig:2.7.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "cda5c3c3a6426b1b790d4b41c86c875709f87f93",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "daa96d0fa343ecaabe39c6c394922a428580ab2b",
  )


  # org.sharegov:mjson:bundle:1.4.0
  native.maven_jar(
      name = "junit_junit",
      artifact = "junit:junit:4.12",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2973d150c0dc1fefe998f834810d68f278ea58ec",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "5e6959e041bdf10a236d888dba83a236153edcd8",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "io_reactivex_rxjava",
      artifact = "io.reactivex:rxjava:1.3.2",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "bcc561a84883a9f3f84e209662c4765a0c3dc691",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "com_datastax_cassandra_cassandra_driver_core",
      artifact = "com.datastax.cassandra:cassandra-driver-core:3.3.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "5b47e34195d97b3a78f2b88e716665ca6f2e180f",
  )


  # pom.xml got requested version
  # ai.grakn:grakn-core:pom:1.4.0-SNAPSHOT
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT got requested version
  # org.slf4j:log4j-over-slf4j:jar:1.7.20 got requested version
  # org.slf4j:jcl-over-slf4j:jar:1.7.20 got requested version
  # ch.qos.logback:logback-classic:jar:1.2.3 wanted version 1.7.25
  native.maven_jar(
      name = "org_slf4j_slf4j_api",
      artifact = "org.slf4j:slf4j-api:1.7.20",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "867d63093eff0a0cb527bf13d397d850af3dcae3",
  )


  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  native.maven_jar(
      name = "org_antlr_antlr4_runtime",
      artifact = "org.antlr:antlr4-runtime:4.5",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "29e48af049f17dd89153b83a7ad5d01b3b4bcdda",
  )


  # org.antlr:antlr-runtime:jar:3.2
  native.maven_jar(
      name = "org_antlr_stringtemplate",
      artifact = "org.antlr:stringtemplate:3.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "6fe2e3bb57daebd1555494818909f9664376dd6c",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0 got requested version
  # org.openrdf.sesame:sesame-rio-n3:jar:2.7.10
  # org.openrdf.sesame:sesame-rio-trig:jar:2.7.10 got requested version
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_turtle",
      artifact = "org.openrdf.sesame:sesame-rio-turtle:2.7.10",
      repository = "http://central.maven.org/maven2/",
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
      repository = "http://central.maven.org/maven2/",
      sha1 = "722d5b58cb054a835605fe4d12ae163513f48d2e",
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


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_rdfxml",
      artifact = "org.openrdf.sesame:sesame-rio-rdfxml:2.7.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "ac1c1049e3d7163f8a51d3f76049ca557dc7d719",
  )


  # org.janusgraph:janusgraph-es:jar:0.3.0
  native.maven_jar(
      name = "org_elasticsearch_client_elasticsearch_rest_client",
      artifact = "org.elasticsearch.client:elasticsearch-rest-client:6.0.1",
      repository = "http://central.maven.org/maven2/",
      sha1 = "d1fd4aad149d91e91efc36a4a0fafc61a8d04cd6",
  )


  # org.reflections:reflections:jar:0.9.9-RC1 wanted version 11.0.2
  # com.netflix.astyanax:astyanax-recipes:jar:3.8.0 wanted version 15.0
  # ai.grakn:grakn-graql:jar:1.4.0-SNAPSHOT
  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 15.0
  # org.janusgraph:janusgraph-core:jar:0.3.0 wanted version 18.0
  # com.datastax.cassandra:cassandra-driver-core:jar:3.3.2 got requested version
  native.maven_jar(
      name = "com_google_guava_guava",
      artifact = "com.google.guava:guava:19.0",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "6ce200f6b23222af3d8abb6b6459e6c44f4bb0e9",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  native.maven_jar(
      name = "com_codahale_metrics_metrics_graphite",
      artifact = "com.codahale.metrics:metrics-graphite:3.0.1",
      repository = "https://oss.sonatype.org/content/repositories/releases/",
      sha1 = "2389e1501d8b9b1ab3b2cd8da16afef56430ba15",
  )


  # com.netflix.astyanax:astyanax-cassandra:jar:3.8.0
  native.maven_jar(
      name = "org_codehaus_jackson_jackson_mapper_asl",
      artifact = "org.codehaus.jackson:jackson-mapper-asl:1.9.2",
      repository = "http://central.maven.org/maven2/",
      sha1 = "95400a7922ce75383866eb72f6ef4a7897923945",
  )


  # org.janusgraph:janusgraph-hadoop:jar:0.3.0
  native.maven_jar(
      name = "org_openrdf_sesame_sesame_rio_n3",
      artifact = "org.openrdf.sesame:sesame-rio-n3:2.7.10",
      repository = "http://central.maven.org/maven2/",
      sha1 = "feb8c7abd4c10230872ef93ab9f08185edf52b5c",
  )


  # org.janusgraph:janusgraph-core:jar:0.3.0
  # com.netflix.astyanax:astyanax-core:jar:3.8.0 wanted version 1.1.2
  native.maven_jar(
      name = "com_github_stephenc_high_scale_lib_high_scale_lib",
      artifact = "com.github.stephenc.high-scale-lib:high-scale-lib:1.1.4",
      repository = "http://central.maven.org/maven2/",
      sha1 = "093865cc75c598f67a7a98e259b2ecfceec9a132",
  )




def generated_java_libraries():
  native.java_library(
      name = "com_google_code_findbugs_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_findbugs_annotations//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
      ],
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
      ],
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
      name = "com_carrotsearch_hppc",
      visibility = ["//visibility:public"],
      exports = ["@com_carrotsearch_hppc//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jffi",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jffi//jar"],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_es",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_es//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_classic",
          ":org_antlr_antlr_runtime",
          ":org_antlr_stringtemplate",
          ":org_elasticsearch_client_elasticsearch_rest_client",
          ":org_janusgraph_janusgraph_core",
      ],
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
      name = "com_google_auto_value_auto_value",
      visibility = ["//visibility:public"],
      exports = ["@com_google_auto_value_auto_value//jar"],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_thrift",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_thrift//jar"],
      runtime_deps = [
          ":com_netflix_astyanax_astyanax_cassandra",
          ":com_netflix_astyanax_astyanax_core",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_xerial_snappy_snappy_java",
      ],
  )


  native.java_library(
      name = "com_thoughtworks_paranamer_paranamer",
      visibility = ["//visibility:public"],
      exports = ["@com_thoughtworks_paranamer_paranamer//jar"],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_cassandra",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_cassandra//jar"],
      runtime_deps = [
          ":com_github_stephenc_high_scale_lib_high_scale_lib",
          ":com_google_guava_guava",
          ":com_netflix_astyanax_astyanax_cassandra",
          ":com_netflix_astyanax_astyanax_core",
          ":com_netflix_astyanax_astyanax_recipes",
          ":com_netflix_astyanax_astyanax_thrift",
          ":joda_time_joda_time",
          ":net_jpountz_lz4_lz4",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_janusgraph_janusgraph_core",
          ":org_xerial_snappy_snappy_java",
      ],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_core",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_core//jar"],
      runtime_deps = [
          ":com_github_stephenc_high_scale_lib_high_scale_lib",
          ":com_google_guava_guava",
          ":joda_time_joda_time",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_util",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_util//jar"],
  )


  native.java_library(
      name = "org_codehaus_jettison_jettison",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jettison_jettison//jar"],
  )


  native.java_library(
      name = "org_xerial_snappy_snappy_java",
      visibility = ["//visibility:public"],
      exports = ["@org_xerial_snappy_snappy_java//jar"],
  )


  native.java_library(
      name = "io_netty_netty_common",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_common//jar"],
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
          ":info_ganglia_gmetric4j_gmetric4j",
          ":org_javassist_javassist",
          ":org_locationtech_jts_jts_core",
          ":org_locationtech_spatial4j_spatial4j",
          ":org_noggit_noggit",
          ":org_reflections_reflections",
      ],
  )


  native.java_library(
      name = "org_janusgraph_janusgraph_hbase",
      visibility = ["//visibility:public"],
      exports = ["@org_janusgraph_janusgraph_hbase//jar"],
  )


  native.java_library(
      name = "org_reflections_reflections",
      visibility = ["//visibility:public"],
      exports = ["@org_reflections_reflections//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":org_javassist_javassist",
      ],
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
      name = "org_locationtech_jts_jts_core",
      visibility = ["//visibility:public"],
      exports = ["@org_locationtech_jts_jts_core//jar"],
  )


  native.java_library(
      name = "org_antlr_antlr_runtime",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_antlr_runtime//jar"],
      runtime_deps = [
          ":org_antlr_stringtemplate",
      ],
  )


  native.java_library(
      name = "org_hamcrest_hamcrest_core",
      visibility = ["//visibility:public"],
      exports = ["@org_hamcrest_hamcrest_core//jar"],
  )


  native.java_library(
      name = "org_noggit_noggit",
      visibility = ["//visibility:public"],
      exports = ["@org_noggit_noggit//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_ntriples",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_ntriples//jar"],
      runtime_deps = [
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
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_common",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "net_jpountz_lz4_lz4",
      visibility = ["//visibility:public"],
      exports = ["@net_jpountz_lz4_lz4//jar"],
  )


  native.java_library(
      name = "com_boundary_high_scale_lib",
      visibility = ["//visibility:public"],
      exports = ["@com_boundary_high_scale_lib//jar"],
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
      name = "com_fasterxml_jackson_module_jackson_module_paranamer",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_module_jackson_module_paranamer//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_databind",
          ":com_thoughtworks_paranamer_paranamer",
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
      name = "org_codehaus_jackson_jackson_core_asl",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jackson_jackson_core_asl//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_trix",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_trix//jar"],
      runtime_deps = [
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
      name = "com_fasterxml_jackson_core_jackson_databind",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_databind//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
      ],
  )


  native.java_library(
      name = "org_javassist_javassist",
      visibility = ["//visibility:public"],
      exports = ["@org_javassist_javassist//jar"],
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
      name = "org_scala_lang_scala_library",
      visibility = ["//visibility:public"],
      exports = ["@org_scala_lang_scala_library//jar"],
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
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_transport",
          ":org_antlr_antlr_runtime",
          ":org_antlr_stringtemplate",
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
          ":org_scala_lang_scala_reflect",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_core",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_core//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jnr_x86asm",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_x86asm//jar"],
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
      name = "org_abego_treelayout_org_abego_treelayout_core",
      visibility = ["//visibility:public"],
      exports = ["@org_abego_treelayout_org_abego_treelayout_core//jar"],
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
      name = "com_codahale_metrics_metrics_core",
      visibility = ["//visibility:public"],
      exports = ["@com_codahale_metrics_metrics_core//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jnr_constants",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_constants//jar"],
  )


  native.java_library(
      name = "com_netflix_astyanax_astyanax_cassandra",
      visibility = ["//visibility:public"],
      exports = ["@com_netflix_astyanax_astyanax_cassandra//jar"],
      runtime_deps = [
          ":com_netflix_astyanax_astyanax_core",
          ":org_codehaus_jackson_jackson_core_asl",
          ":org_codehaus_jackson_jackson_mapper_asl",
          ":org_codehaus_jettison_jettison",
          ":org_xerial_snappy_snappy_java",
      ],
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
      name = "io_netty_netty_buffer",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_buffer//jar"],
      runtime_deps = [
          ":io_netty_netty_common",
      ],
  )


  native.java_library(
      name = "ch_qos_logback_logback_core",
      visibility = ["//visibility:public"],
      exports = ["@ch_qos_logback_logback_core//jar"],
  )


  native.java_library(
      name = "com_github_jnr_jnr_ffi",
      visibility = ["//visibility:public"],
      exports = ["@com_github_jnr_jnr_ffi//jar"],
      runtime_deps = [
          ":com_github_jnr_jffi",
          ":com_github_jnr_jnr_x86asm",
      ],
  )


  native.java_library(
      name = "info_ganglia_gmetric4j_gmetric4j",
      visibility = ["//visibility:public"],
      exports = ["@info_ganglia_gmetric4j_gmetric4j//jar"],
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
      name = "org_openrdf_sesame_sesame_model",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_model//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_util",
      ],
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
      name = "org_openrdf_sesame_sesame_rio_api",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_api//jar"],
      runtime_deps = [
          ":org_openrdf_sesame_sesame_model",
          ":org_openrdf_sesame_sesame_util",
      ],
  )


  native.java_library(
      name = "io_reactivex_rxjava",
      visibility = ["//visibility:public"],
      exports = ["@io_reactivex_rxjava//jar"],
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
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "org_slf4j_slf4j_api",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_slf4j_api//jar"],
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
      name = "org_antlr_stringtemplate",
      visibility = ["//visibility:public"],
      exports = ["@org_antlr_stringtemplate//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_turtle",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_turtle//jar"],
      runtime_deps = [
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
      name = "com_fasterxml_jackson_core_jackson_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_annotations//jar"],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_rdfxml",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_rdfxml//jar"],
      runtime_deps = [
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
  )


  native.java_library(
      name = "com_google_guava_guava",
      visibility = ["//visibility:public"],
      exports = ["@com_google_guava_guava//jar"],
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
      name = "org_codehaus_jackson_jackson_mapper_asl",
      visibility = ["//visibility:public"],
      exports = ["@org_codehaus_jackson_jackson_mapper_asl//jar"],
      runtime_deps = [
          ":org_codehaus_jackson_jackson_core_asl",
      ],
  )


  native.java_library(
      name = "org_openrdf_sesame_sesame_rio_n3",
      visibility = ["//visibility:public"],
      exports = ["@org_openrdf_sesame_sesame_rio_n3//jar"],
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
      name = "com_github_stephenc_high_scale_lib_high_scale_lib",
      visibility = ["//visibility:public"],
      exports = ["@com_github_stephenc_high_scale_lib_high_scale_lib//jar"],
  )
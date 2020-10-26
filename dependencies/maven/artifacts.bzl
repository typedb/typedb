#
# Copyright (C) 2020 Grakn Labs
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

artifacts = [
    "ch.qos.logback:logback-classic",
    "com.google.code.findbugs:jsr305",
    "com.google.protobuf:protobuf-java",
    "com.quantego:clp-java",
    "info.picocli:picocli",
    "io.cucumber:cucumber-java",
    "io.cucumber:cucumber-junit",
    "io.grpc:grpc-api",
    "io.grpc:grpc-netty",
    "io.grpc:grpc-stub",
    "io.netty:netty-all",
    "junit:junit",
    "org.rocksdb:rocksdbjni",
    "org.rocksdb:rocksdbjni-dev",
    "org.slf4j:slf4j-api",
]

overrides = {
    "io.netty:netty-all": "4.1.38.Final",
    "io.netty:netty-buffer": "4.1.38.Final",
    "io.netty:netty-codec": "4.1.38.Final",
    "io.netty:netty-codec-http2": "4.1.38.Final",
    "io.netty:netty-codec-http": "4.1.38.Final",
    "io.netty:netty-codec-socks": "4.1.38.Final",
    "io.netty:netty-common": "4.1.38.Final",
    "io.netty:netty-handler": "4.1.38.Final",
    "io.netty:netty-handler-proxy": "4.1.38.Final",
    "io.netty:netty-resolver": "4.1.38.Final",
    "io.netty:netty-transport": "4.1.38.Final",
}

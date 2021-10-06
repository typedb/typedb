#
# Copyright (C) 2021 Vaticle
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

assemble_files_mac_linux = {
    "//server/conf/mac-linux:logback": "server/conf/logback.xml",
    "//server/conf/mac-linux:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf/mac-linux:typedb-properties": "server/conf/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

assemble_files_windows = {
    "//server/conf/windows:logback": "server/conf/logback.xml",
    "//server/conf/windows:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf/windows:typedb-properties": "server/conf/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

permissions_mac_linux = {
    "server/conf/typedb.properties": "0755",
    "server/conf/logback.xml": "0755",
    "server/conf/logback-debug.xml": "0755",
}

permissions_windows = {
    "server/conf/typedb.properties": "0755",
    "server/conf/ogback.xml": "0755",
    "server/conf/logback-debug.xml": "0755",
}

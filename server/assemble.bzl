
assemble_files_mac_linux = {
    "//server/conf/mac-linux:logback": "server/conf/mac-linux/logback.xml",
    "//server/conf/mac-linux:logback-debug": "server/conf/mac-linux/logback-debug.xml",
    "//server/conf/mac-linux:typedb-properties": "server/conf/mac-linux/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

assemble_files_windows = {
    "//server/conf/windows:logback": "server/conf/windows/logback.xml",
    "//server/conf/windows:logback-debug": "server/conf/windows/logback-debug.xml",
    "//server/conf/windows:typedb-properties": "server/conf/windows/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

permissions_mac_linux = {
    "server/conf/mac-linux/typedb.properties": "0755",
    "server/conf/mac-linux/logback.xml": "0755",
    "server/conf/mac-linux/logback-debug.xml": "0755",
}

permissions_windows = {
    "server/conf/windows/typedb.properties": "0755",
    "server/conf/windows/logback.xml": "0755",
    "server/conf/windows/logback-debug.xml": "0755",
}
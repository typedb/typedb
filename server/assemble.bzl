
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
/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.server.common;

import com.vaticle.typedb.common.util.OS;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ServerDefaults {

    public static final Path TYPEDB_DIR = getTypedbDir();
    public static final Path DATA_DIR = TYPEDB_DIR.resolve("server/data");
    public static final Path LOGS_DIR = TYPEDB_DIR.resolve("server/logs");
    public static final File PROPERTIES_FILE = OS.detect() == OS.WINDOWS ?
            TYPEDB_DIR.resolve("server/conf/windows/typedb.properties").toFile() :
            TYPEDB_DIR.resolve("server/conf/mac-linux/typedb.properties").toFile();
    public static final File ASCII_LOGO_FILE = TYPEDB_DIR.resolve("server/resources/typedb-ascii.txt").toFile();
    public static final int DEFAULT_DATABASE_PORT = 1729;

    private static Path getTypedbDir() {
        String homeDir;
        if ((homeDir = System.getProperty("typedb.dir")) == null) {
            homeDir = System.getProperty("user.dir");
        }
        return Paths.get(homeDir);
    }
}

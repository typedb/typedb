/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ServerDefaults {

    public static final Path GRAKN_DIR = getGraknDir();
    public static final Path DATA_DIR = GRAKN_DIR.resolve("server/data");
    public static final Path LOGS_DIR = GRAKN_DIR.resolve("server/logs");
    public static final File PROPERTIES_FILE = GRAKN_DIR.resolve("server/conf/grakn.properties").toFile();
    public static final File ASCII_LOGO_FILE = GRAKN_DIR.resolve("server/resources/grakn-core-ascii.txt").toFile();
    public static final int DEFAULT_DATABASE_PORT = 1729;

    private static Path getGraknDir() {
        String homeDir;
        if ((homeDir = System.getProperty("grakn.dir")) == null) {
            homeDir = System.getProperty("user.dir");
        }
        return Paths.get(homeDir);
    }
}

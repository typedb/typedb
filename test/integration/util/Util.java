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
 */

package grakn.core.test.integration.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Util {

    public static void resetDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            System.out.println("Database directory exists!");
            Files.walk(directory).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(directory);

        System.out.println("Database directory created: " + directory.toString());
    }

    public static void assertNotNulls(Object... objects) {
        for (Object object : objects) {
            assertNotNull(object);
        }
    }

    public static void assertNulls(Object... objects) {
        for (Object object : objects) {
            assertNull(object);
        }
    }
}

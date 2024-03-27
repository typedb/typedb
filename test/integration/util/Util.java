/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.integration.util;

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
            deleteDirectory(directory);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(directory);
        System.out.println("Database directory created: " + directory);
    }

    public static void deleteDirectory(Path directory) throws IOException {
        Files.walk(directory).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
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

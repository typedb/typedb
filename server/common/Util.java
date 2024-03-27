/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.server.common.Constants.ASCII_LOGO_FILE;

public class Util {

    public static void printASCIILogo() throws IOException {
        if (ASCII_LOGO_FILE.exists()) {
            System.out.println("\n" + new String(Files.readAllBytes(ASCII_LOGO_FILE.toPath()), StandardCharsets.UTF_8));
        }
    }

    public static Path getTypedbDir() {
        String homeDir;
        if ((homeDir = System.getProperty("typedb.dir")) == null) {
            homeDir = System.getProperty("user.dir");
        }
        return Paths.get(homeDir);
    }
}

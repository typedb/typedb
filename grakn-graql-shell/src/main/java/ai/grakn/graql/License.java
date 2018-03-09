/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql;

import jline.console.ConsoleReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Handles printing the license for {@link GraqlShell}.
 *
 * @author Felix Chapman
 */
public class License {

    private static final String LICENSE_PROMPT = "\n" +
            "Grakn  Copyright (C) 2018  Grakn Labs Limited \n" +
            "This is free software, and you are welcome to redistribute it \n" +
            "under certain conditions; type 'license' for details.\n";

    private static final String LICENSE_LOCATION = "LICENSE.txt";

    public License() {
    }

    static void printLicensePrompt(ConsoleReader console) throws IOException {
        console.print(LICENSE_PROMPT);
    }

    static void printLicense(ConsoleReader console) throws IOException {
        StringBuilder result = new StringBuilder("");

        //Get file from resources folder
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(LICENSE_LOCATION);

        Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name());
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.append(line).append("\n");
        }
        result.append("\n");
        scanner.close();

        console.println(result.toString());
    }
}
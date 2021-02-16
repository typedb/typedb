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

package grakn.core.test.assembly;

import grakn.common.test.console.ConsoleRunner;
import grakn.common.test.server.GraknCoreRunner;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public class AssemblyTest {

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        GraknCoreRunner server = new GraknCoreRunner();
        try {
            server.start();
        } finally {
            server.stop();
        }
    }

    @Test
    public void console() throws InterruptedException, TimeoutException, IOException {
        GraknCoreRunner server = new GraknCoreRunner();
        try {
            server.start();
            ConsoleRunner console = new ConsoleRunner();
            int exitCode = console.run(server.address(), false, Paths.get("test/assembly/console-script"));
            assert exitCode == 0;
        } finally {
            server.stop();
        }
    }
}

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

package com.vaticle.typedb.core.test.assembly;

import com.vaticle.typedb.common.test.console.TypeDBConsoleRunner;
import com.vaticle.typedb.common.test.server.TypeDBCoreRunner;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public class AssemblyTest {

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner();
        try {
            server.start();
        } finally {
            server.stop();
        }
    }

    @Test
    public void console() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner();
        try {
            server.start();
            TypeDBConsoleRunner console = new TypeDBConsoleRunner();
            int exitCode = console.run(
                    "--server", server.address(),
                    "--script", Paths.get("test", "assembly", "console-script").toAbsolutePath().toString()
            );
            assert exitCode == 0;
        } finally {
            server.stop();
        }
    }
}

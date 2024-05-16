/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.assembly;

import com.vaticle.typedb.console.tool.runner.TypeDBConsoleRunner;
import com.vaticle.typedb.core.tool.runner.TypeDBCoreRunner;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;

public class AssemblyTest {

    private static final Map<String, String> TYPEDB_OPTIONS = map(
            pair("--diagnostics.reporting.errors", "false"),
            pair("--diagnostics.reporting.statistics", "false"),
            pair("--diagnostics.monitoring.enable", "false")
    );

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner(TYPEDB_OPTIONS);
        try {
            server.start();
        } finally {
            server.stop();
        }
    }

    @Test
    public void console() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner(TYPEDB_OPTIONS);
        try {
            server.start();
            TypeDBConsoleRunner console = new TypeDBConsoleRunner();
            int exitCode = console.run(
                    "--core", server.address(),
                    "--script", Paths.get("test", "assembly", "console-script").toAbsolutePath().toString()
            );
            assert exitCode == 0;
        } finally {
            server.stop();
        }
    }
}

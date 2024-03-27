/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.encoding.Encoding;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.INCOMPATIBLE_ENCODING;
import static com.vaticle.typedb.core.common.test.Util.assertThrowsWithMessage;

public class DatabaseTest {

    private static final Factory factory = new CoreFactory();

    @BeforeClass
    public static void beforeClass() {
        Diagnostics.Noop.initialise();
    }

    @Test
    public void databaseCreationSucceeds() throws IOException {
        Path dataDir = Files.createTempDirectory("test-dir");
        Path logDir = dataDir.resolve("logs");
        Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
                .storageIndexCacheSize(MB).storageDataCacheSize(MB);
        CoreDatabaseManager databaseMgr = factory.databaseManager(options);
        databaseMgr.create("test");
        databaseMgr.close();
    }

    @Test
    public void incompatibleDataEncodingThrows() {
        Path dataDir = Paths.get("test/integration/database/data");
        Path logDir = dataDir.resolve("logs");
        Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
                .storageIndexCacheSize(MB).storageDataCacheSize(MB);
        assertThrowsWithMessage(
                () -> factory.databaseManager(options),
                INCOMPATIBLE_ENCODING.message("test", dataDir.resolve("test").toAbsolutePath(), 0, Encoding.ENCODING_VERSION)
        );
    }
}

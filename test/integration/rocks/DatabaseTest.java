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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.graph.common.Encoding;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.INCOMPATIBLE_ENCODING;
import static com.vaticle.typedb.core.common.test.Util.assertThrowsWithMessage;

public class DatabaseTest {

    private static final Factory rocksFactory = new RocksFactory();

    @Test
    public void databaseCreationSucceeds() throws IOException {
        Path dataDir = Files.createTempDirectory("test-dir");
        Path logDir = dataDir.resolve("logs");
        Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
                .storageIndexCacheSize(MB).storageDataCacheSize(MB);
        RocksTypeDB typedb = rocksFactory.typedb(options);
        typedb.databases().create("test");
    }

    @Test
    public void incompatibleDataEncodingThrows() {
        Path dataDir = Paths.get("test/integration/rocks/data");
        Path logDir = dataDir.resolve("logs");
        Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
                .storageIndexCacheSize(MB).storageDataCacheSize(MB);

        assertThrowsWithMessage(
                () -> rocksFactory.typedb(options),
                INCOMPATIBLE_ENCODING.message("test", 0, Encoding.ENCODING_VERSION)
        );
    }

}

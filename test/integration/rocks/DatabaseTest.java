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
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;

public class DatabaseTest {

    private static final Factory rocksFactory = new RocksFactory();
    private static final Path dataDir = Paths.get("test/integration/rocks/data");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);

    private RocksTypeDB rocksTypeDB;

    @Before
    public void setUp() {
        rocksTypeDB = rocksFactory.typedb(options);
    }

    @After
    public void tearDown() {
        rocksTypeDB.close();
    }
}

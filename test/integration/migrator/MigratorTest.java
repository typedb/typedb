/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.migrator;

import com.google.protobuf.Parser;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.migrator.data.DataProto;
import com.vaticle.typedb.core.migrator.database.DatabaseExporter;
import com.vaticle.typedb.core.migrator.database.DatabaseImporter;
import com.vaticle.typedb.core.server.Version;
import com.vaticle.typedb.core.test.integration.util.Util;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MigratorTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("migrator-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "typedb";
    private static final Path schemaPath = Paths.get("test/integration/migrator/schema.tql");
    private final Path dataPath = Paths.get("test/integration/migrator/data.typedb");
    private final Path exportDataPath = Paths.get("test/integration/migrator/exported-data.typedb");

    @Test
    public void test_import_export_database() throws IOException {
        Util.resetDirectory(dataDir);
        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            new DatabaseImporter(databaseMgr, database, schemaPath, dataPath, Version.VERSION).run();
            Path exportedSchema = File.createTempFile("exportedSchema", ".tql").toPath();
            new DatabaseExporter(
                    databaseMgr, database, exportedSchema,
                    exportDataPath, Version.VERSION
            ).run();
            assertEquals(trimSchema(readString(schemaPath, UTF_8)), trimSchema(readString(exportedSchema, UTF_8)));
            assertEquals(getChecksums(dataPath), getChecksums(exportDataPath));
        }
    }

    private String trimSchema(String schema) {
        return schema.substring(schema.indexOf("define")).trim();
    }

    private DataProto.Item.Checksums getChecksums(Path path) throws IOException {
        Parser<DataProto.Item> parser = DataProto.Item.parser();
        try (InputStream fileInput = new BufferedInputStream(Files.newInputStream(path))) {
            DataProto.Item item;
            while ((item = parser.parseDelimitedFrom(fileInput)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.CHECKSUMS) {
                    return item.getChecksums();
                }
            }
        }
        fail();
        return null;
    }
}

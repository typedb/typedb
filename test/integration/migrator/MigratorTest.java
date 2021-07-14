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

package com.vaticle.typedb.core.migrator;

import com.google.protobuf.Parser;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options.Database;
//import com.vaticle.typedb.core.migrator.data.DataProto;
//import com.vaticle.typedb.core.migrator.data.DataExporter;
//import com.vaticle.typedb.core.migrator.data.DataImporter;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.server.Version;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MigratorTest {
//
//    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("migrator-test");
//    private static final Path logDir = dataDir.resolve("logs");
//    private static final Database options = new Database().dataDir(dataDir).logsDir(logDir);
//    private static final String database = "typedb";
//    private static final Path schemaPath = Paths.get("test/integration/migrator/schema.tql");
//    private final Path dataPath = Paths.get("test/integration/migrator/data.typedb");
//    private final Path exportDataPath = Paths.get("test/integration/migrator/exported-data.typedb");
//
//    @Test
//    public void test_import_export_schema() throws IOException {
//        Util.resetDirectory(dataDir);
//        try (TypeDB typedb = RocksTypeDB.open(options)) {
//            typedb.databases().create(database);
//            String savedSchema = new String(Files.readAllBytes(schemaPath), UTF_8);
//            runSchema(typedb, savedSchema);
//            String exportedSchema = typedb.databases().get(database).schema();
//            assertEquals(trimSchema(savedSchema), trimSchema(exportedSchema));
//        }
//    }
//
//    @Test
//    public void test_import_export_data() throws IOException {
//        Util.resetDirectory(dataDir);
//        try (RocksTypeDB typedb = RocksTypeDB.open(options)) {
//            typedb.databases().create(database);
//            String schema = new String(Files.readAllBytes(schemaPath), UTF_8);
//            runSchema(typedb, schema);
//            new DataImporter(typedb, database, dataPath, new HashMap<>(), Version.VERSION).run();
//            new DataExporter(typedb, database, exportDataPath, Version.VERSION).run();
//            assertEquals(getChecksums(dataPath), getChecksums(exportDataPath));
//        }
//    }
//
//    private void runSchema(TypeDB typedb, String schema) {
//        try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
//            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
//                TypeQLDefine query = TypeQL.parseQuery(schema);
//                tx.query().define(query);
//                tx.commit();
//            }
//        }
//    }
//
//    private String trimSchema(String schema) {
//        return schema.substring(schema.indexOf("define")).trim();
//    }
//
//    private DataProto.Item.Checksums getChecksums(Path path) throws IOException {
//        Parser<DataProto.Item> parser = DataProto.Item.parser();
//        try (InputStream fileInput = new BufferedInputStream(Files.newInputStream(path))) {
//            DataProto.Item item;
//            while ((item = parser.parseDelimitedFrom(fileInput)) != null) {
//                if (item.getItemCase() == DataProto.Item.ItemCase.CHECKSUMS) {
//                    return item.getChecksums();
//                }
//            }
//        }
//        fail();
//        return null;
//    }
}

/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.migrator;

import com.google.protobuf.Parser;
import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.rocks.RocksGrakn;
import grakn.core.server.migrator.proto.DataProto;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
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

    private static final Path directory = Paths.get(System.getProperty("user.dir")).resolve("migrator-test");
    private static final String database = "grabl";
    private static final Path schemaPath = Paths.get("test/integration/migrator/schema.gql");
    private final Path dataPath = Paths.get("test/integration/migrator/data.grakn");
    private final Path exportDataPath = Paths.get("test/integration/migrator/exported-data.grakn");

    @Test
    public void test_import_export_schema() throws IOException {
        Util.resetDirectory(directory);
        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            String savedSchema = new String(Files.readAllBytes(schemaPath), UTF_8);
            runSchema(grakn, savedSchema);
            String exportedSchema = new Schema(grakn, database).getSchema();
            assertEquals(trimSchema(savedSchema), trimSchema(exportedSchema));
        }
    }

    @Test
    public void test_import_export_data() throws IOException {
        Util.resetDirectory(directory);
        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            String schema = new String(Files.readAllBytes(schemaPath), UTF_8);
            runSchema(grakn, schema);
            Importer importer = new Importer(grakn, database, dataPath, new HashMap<>());
            importer.run();
            Exporter exporter = new Exporter(grakn, database, exportDataPath);
            exporter.run();
            assertEquals(getChecksums(dataPath), getChecksums(exportDataPath));
        }
    }

    private void runSchema(Grakn grakn, String schema) {
        try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                final GraqlDefine query = Graql.parseQuery(schema);
                tx.query().define(query);
                tx.commit();
            }
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

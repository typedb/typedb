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
 */

package grakn.core.migration;

import com.google.protobuf.Parser;
import grakn.client.GraknClient;
import grakn.core.server.migrate.proto.DataProto;
import graql.lang.Graql;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static grakn.core.migration.MigrationE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.migration.MigrationE2EConstants.assertGraknIsNotRunning;
import static grakn.core.migration.MigrationE2EConstants.assertGraknIsRunning;
import static grakn.core.migration.MigrationE2EConstants.assertZipExists;
import static grakn.core.migration.MigrationE2EConstants.unzipGrakn;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MigrationE2E {

    private static final Path DATA_ROOT = Paths.get("test-end-to-end", "migration", "data");
    private static final Path SCHEMA = DATA_ROOT.resolve("schema.gql");
    private static final Path SCHEMA_PT2 = DATA_ROOT.resolve("schema-pt2.gql");
    private static final Path IMPORT_PATH = DATA_ROOT.resolve("simulation.grakn");
    private static final Path SCHEMA_EXPORT_TRUTH = DATA_ROOT.resolve("schema_export_truth.gql");

    private static final ProcessExecutor COMMAND_EXECUTOR = new ProcessExecutor()
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    private static final String GRAKN = GRAKN_UNZIPPED_DIRECTORY.resolve("grakn").toString();

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertZipExists();
        unzipGrakn();
        assertGraknIsNotRunning();
        COMMAND_EXECUTOR.command(GRAKN, "server", "start").execute();
        assertGraknIsRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        COMMAND_EXECUTOR.command(GRAKN, "server", "stop").execute();
        assertGraknIsNotRunning();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void testMigration() throws InterruptedException, TimeoutException, IOException {
        System.out.println("Loading schema");
        loadSimulationSchema("simulation");

        System.out.println("Performing import");
        assertExecutes(GRAKN, "server", "import", "--no-anim", "simulation", IMPORT_PATH.toString());

        System.out.println("Smoke test that something was inserted");
        GraknClient graknClient = new GraknClient("localhost:48555");
        GraknClient.Session session = graknClient.session("simulation");
        try (GraknClient.Transaction tx = session.transaction().read()) {
            assertThat(tx.execute(Graql.compute().count().in("thing")).get(0).number().longValue(), greaterThan(0L));
        }

        Path exportPath = Paths.get("simulationexport.grakn");

        System.out.println("Performing export");
        assertExecutes(GRAKN, "server", "export", "--no-anim", "simulation", exportPath.toString());

        assertTrue(Files.exists(exportPath));

        System.out.println("Checking checksums...");
        assertEquals(getChecksums(IMPORT_PATH), getChecksums(exportPath));
        System.out.println("Checksums OK!");
    }

    @Test
    public void testSchemaExport() throws InterruptedException, IOException, TimeoutException {
        loadSimulationSchema("schematest");

        ProcessResult result = assertExecutes(GRAKN, "server", "schema", "schematest");

        String export = result.getOutput().getString();
        String truth = Files.lines(SCHEMA_EXPORT_TRUTH, StandardCharsets.UTF_8).collect(Collectors.joining("\n"));

        assertThat(export, equalTo(truth));
    }

    private void loadSimulationSchema(String keyspace) throws InterruptedException, TimeoutException, IOException {
        assertExecutes(GRAKN, "console", "-k", keyspace, "-f", SCHEMA.toString(), "-f", SCHEMA_PT2.toString());
    }

    private DataProto.Item.Checksums getChecksums(Path path) throws IOException {
        Parser<DataProto.Item> parser = DataProto.Item.parser();
        InputStream fileInput = new BufferedInputStream(Files.newInputStream(path));

        DataProto.Item item;
        while ((item = parser.parseDelimitedFrom(fileInput)) != null) {
            if (item.getItemCase() == DataProto.Item.ItemCase.CHECKSUMS) {
                return item.getChecksums();
            }
        }

        fail();
        return null;
    }

    private static ProcessResult assertExecutes(String... args) throws InterruptedException, TimeoutException, IOException {
        ProcessResult result = COMMAND_EXECUTOR.command(args).execute();
        assertEquals(0, result.getExitValue());
        return result;
    }
}

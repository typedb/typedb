/*
 * Copyright (C) 2023 Vaticle
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

package com.vaticle.typedb.core.server;

import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.server.parameters.CoreConfig;
import com.vaticle.typedb.core.server.parameters.CoreConfigFactory;
import com.vaticle.typedb.core.server.parameters.CoreConfigParser;
import com.vaticle.typedb.core.server.parameters.util.Option;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_ARCHIVE_EXT;
import static com.vaticle.typedb.core.test.integration.util.Util.deleteDirectory;
import static org.junit.Assert.assertEquals;

public class ParametersTest {

    private static final Path CONFIG_PATH_DEFAULT = Paths.get("server/parameters/config.yml");

    @Test
    public void test() throws IOException, InterruptedException, TypeDBCheckedException {
        Path logDir = null;
        Path dataDir = null;
        TypeDBServer typeDBServer = null;
        try {
            logDir = Files.createTempDirectory("log-file-tmp").toFile().toPath();
            dataDir = Files.createTempDirectory("data-tmp").toFile().toPath();

            CoreConfig config = CoreConfigFactory.config(
                    CONFIG_PATH_DEFAULT,
                    set(
                            new Option("storage.data", dataDir.toAbsolutePath().toString()),
                            new Option("log.output.file.base-dir", logDir.toAbsolutePath().toString()),
                            new Option("log.output.file.archive-grouping", "minute"),
                            new Option("log.output.file.archive-age-limit", "2 minutes"),
                            new Option("log.logger.default.level", "trace"),
                            new Option("log.logger.storage.level", "trace")
                    ),
                    new CoreConfigParser()
            );

            typeDBServer = TypeDBServer.create(config, false);
            typeDBServer.databaseMgr.create("test1");
            Thread.sleep(1 * 60 * 1_000L);
            typeDBServer.databaseMgr.create("test2");
            Thread.sleep(1 * 60 * 1_000L);
            typeDBServer.databaseMgr.create("test3");
            Thread.sleep(1 * 60 * 1_000L);
            List<Path> logArchives = getLogArchives(logDir);
            System.out.println("Found log archives:");
            System.out.println(logArchives.toString());
            assertEquals(2, logArchives.size());
        } finally {
            if (logDir != null) deleteDirectory(logDir);
            if (dataDir != null) deleteDirectory(dataDir);
            if (typeDBServer != null) typeDBServer.close();
        }
    }

    private List<Path> getLogArchives(Path logDir) throws IOException {
        return Files.list(logDir).filter(p -> p.toString().endsWith(TYPEDB_LOG_ARCHIVE_EXT)).collect(Collectors.toList());
    }
}


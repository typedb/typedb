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

public class ParametersTest {

    @Test
    public void test() throws IOException {

        Path logDir = null;
        try {
            logDir = Files.createTempDirectory("log-file-tmp").toFile().toPath();

            CoreConfig config = CoreConfigFactory.config(
                    set(
                            new Option("log.output.file.base-dir", logDir.toAbsolutePath().toString()),
                            new Option("log.output.file.archive-grouping", "minute"),
                            new Option("log.output.file.archive-age-limit", "2"),
                            new Option("log.logger.default.level", "trace")
                    ),
                    new CoreConfigParser()
            );

            TypeDBServer typedDBServer = TypeDBServer.create(config, false);

            getLogArchives(logDir);




        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (logDir != null) {
                Files.deleteIfExists(Paths.get(logDir));
            }
        }
    }

    private List<Path> getLogArchives(Path logDir) throws IOException {
        return Files.list(logDir).filter(p -> p.endsWith(TYPEDB_LOG_ARCHIVE_EXT)).collect(Collectors.toList());
    }
}


/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.server.common.Constants.*;
import static com.vaticle.typedb.core.test.integration.util.Util.deleteDirectory;
import static org.junit.Assert.assertEquals;

public class ParametersTest {

    private static final Path CONFIG_PATH_DEFAULT = Paths.get("server/parameters/config.yml");

//    @Test
//    public void test_log_archive_age_limits() throws IOException, InterruptedException, TypeDBCheckedException {
//        Path logDir = null;
//        Path dataDir = null;
//        TypeDBServer typeDBServer = null;
//        try {
//            logDir = Files.createTempDirectory("log-file-tmp").toFile().toPath();
//            dataDir = Files.createTempDirectory("data-tmp").toFile().toPath();
//
//            Set<Option> testOptions = getGeneralTestOptions(dataDir, logDir);
//            testOptions.addAll(set(
//                    new Option("log.output.file.archive-grouping", "minute"),
//                    new Option("log.output.file.archive-age-limit", "2 minutes")
//            ));
//
//            CoreConfig config = CoreConfigFactory.config(CONFIG_PATH_DEFAULT, testOptions, new CoreConfigParser());
//
//            typeDBServer = TypeDBServer.create(config, false);
//            typeDBServer.databaseMgr.create("test1");
//            Thread.sleep(1 * 60 * 1_000L);
//            typeDBServer.databaseMgr.create("test2");
//            Thread.sleep(1 * 60 * 1_000L);
//            typeDBServer.databaseMgr.create("test3");
//            Thread.sleep(1 * 60 * 1_000L);
//            List<Path> logArchives = getLogArchives(logDir);
//            System.out.println("Found log archives:");
//            System.out.println(logArchives.toString());
//            assertEquals(2, logArchives.size());
//        } finally {
//            if (typeDBServer != null) typeDBServer.close();
//            if (dataDir != null) deleteDirectory(dataDir);
//            if (logDir != null) deleteDirectory(logDir);
//        }
//    }

    @Test
    public void test_deployment_server_id_getting() throws IOException, InterruptedException, TypeDBCheckedException {
        Path logDir = null;
        Path dataDir = null;
        TypeDBServer typeDBServer = null;
        try {
            logDir = Files.createTempDirectory("log-file-tmp").toFile().toPath();
            dataDir = Files.createTempDirectory("data-tmp").toFile().toPath();

            // Deployment ID: get from the config
            // Server ID: generate and save into a file
            Set<Option> testOptions = getGeneralTestOptions(dataDir, logDir);
            testOptions.add(new Option("diagnostics.deployment-id", "TESTVALUEID01"));

            CoreConfig config = CoreConfigFactory.config(CONFIG_PATH_DEFAULT, testOptions, new CoreConfigParser());
            typeDBServer = TypeDBServer.create(config, false);
            typeDBServer.databaseMgr.create("test1");

            List<Path> serverIdFiles = getServerIdFiles(dataDir);
            assertEquals(1, serverIdFiles.size());
            String savedServerId1 = Files.readString(serverIdFiles.get(0));

            List<Path> deploymentIdFiles = getDeploymentIdFiles(dataDir);
            assertEquals(0, deploymentIdFiles.size());

            typeDBServer.close();

            // Deployment ID: get from server ID (absent in the config), save into a file
            // Server ID: get from the old file
            testOptions = getGeneralTestOptions(dataDir, logDir);

            config = CoreConfigFactory.config(CONFIG_PATH_DEFAULT, testOptions, new CoreConfigParser());

            typeDBServer = TypeDBServer.create(config, false);
            typeDBServer.databaseMgr.create("test2");

            serverIdFiles = getServerIdFiles(dataDir);
            assertEquals(1, serverIdFiles.size());
            String savedServerId2 = Files.readString(serverIdFiles.get(0));
            assertEquals(savedServerId1, savedServerId2);

            deploymentIdFiles = getDeploymentIdFiles(dataDir);
            assertEquals(1, deploymentIdFiles.size());
            String savedDeploymentId = Files.readString(deploymentIdFiles.get(0));
            assertEquals(savedServerId2, savedDeploymentId);
        } finally {
            if (typeDBServer != null) typeDBServer.close();
            if (dataDir != null) deleteDirectory(dataDir);
            if (logDir != null) deleteDirectory(logDir);
        }
    }

    private Set<Option> getGeneralTestOptions(Path dataDir, Path logDir) {
        return new HashSet<>(set(
                new Option("storage.data", dataDir.toAbsolutePath().toString()),
                new Option("log.output.file.base-dir", logDir.toAbsolutePath().toString()),
                new Option("log.logger.default.level", "trace"),
                new Option("log.logger.storage.level", "trace"),
                new Option("diagnostics.reporting.errors", "false"),
                new Option("diagnostics.reporting.statistics", "false")));
    }

    private List<Path> getLogArchives(Path logDir) throws IOException {
        return Files.list(logDir).filter(p -> p.toString().endsWith(TYPEDB_LOG_ARCHIVE_EXT)).collect(Collectors.toList());
    }

    private List<Path> getServerIdFiles(Path dataDir) throws IOException {
        return Files.list(dataDir).filter(p -> p.toString().endsWith(SERVER_ID_FILE_NAME)).collect(Collectors.toList());
    }

    private List<Path> getDeploymentIdFiles(Path dataDir) throws IOException {
        return Files.list(dataDir).filter(p -> p.toString().endsWith(DEPLOYMENT_ID_FILE_NAME)).collect(Collectors.toList());
    }
}


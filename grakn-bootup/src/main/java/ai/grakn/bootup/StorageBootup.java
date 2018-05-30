/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.bootup;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class responsible for managing the bootup-related process for the Storage component, including
 * starting and stopping, performing status checks, and cleaning the data.
 *
 * The PID file for the Storage component is managed internally by Cassandra and not by this class. This means that
 * you will not find any code which creates or deletes the PID file for the Storage component.
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class StorageBootup {

    private static final String DISPLAY_NAME = "Storage";
    private static final String STORAGE_PROCESS_NAME = "CassandraDaemon";
    private static final long STORAGE_STARTUP_TIMEOUT_SECOND = 60;
    private static final Path STORAGE_PIDFILE = Paths.get(File.separator,"tmp","grakn-storage.pid");
    private static final Path STORAGE_BIN = Paths.get("services", "cassandra", "cassandra");
    private static final Path NODETOOL_BIN = Paths.get("services", "cassandra", "nodetool");
    private static final Path STORAGE_DATA = Paths.get("db", "cassandra");

    private BootupProcessExecutor bootupProcessExecutor;
    private final Path graknHome;
    private final GraknConfig graknProperties;

    public StorageBootup(BootupProcessExecutor bootupProcessExecutor, Path graknHome, Path graknPropertiesPath) {
        this.graknHome = graknHome;
        this.graknProperties = GraknConfig.read(graknPropertiesPath.toFile());
        this.bootupProcessExecutor = bootupProcessExecutor;
    }

    /**
     * Attempt to start Storage if it is not already running
     */
    public void startIfNotRunning() {
        boolean isStorageRunning = bootupProcessExecutor.isProcessRunning(STORAGE_PIDFILE);
        if (isStorageRunning) {
            System.out.println(DISPLAY_NAME + " is already running");
        } else {
            FileUtils.deleteQuietly(STORAGE_PIDFILE.toFile()); // delete dangling STORAGE_PIDFILE, if any
            start();
        }
    }

   public void stop() {
       bootupProcessExecutor.stopProcessIfRunning(STORAGE_PIDFILE, DISPLAY_NAME);
    }

    public void status() {
        bootupProcessExecutor.processStatus(STORAGE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME +" pid = '"+ bootupProcessExecutor.getPidFromFile(STORAGE_PIDFILE).orElse("") +
                "' (from "+ STORAGE_PIDFILE +"), '"+ bootupProcessExecutor.getPidFromPsOf(STORAGE_PROCESS_NAME) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+ DISPLAY_NAME +"...");
        System.out.flush();
        try (Stream<Path> files = Files.walk(STORAGE_DATA)) {
            files.map(Path::toFile)
                    .sorted(Comparator.comparing(File::isDirectory))
                    .forEach(File::delete);
            Files.createDirectories(graknHome.resolve(STORAGE_DATA).resolve("data"));
            Files.createDirectories(graknHome.resolve(STORAGE_DATA).resolve("commitlog"));
            Files.createDirectories(graknHome.resolve(STORAGE_DATA).resolve("saved_caches"));
            System.out.println("SUCCESS");
        } catch (IOException e) {
            System.out.println("FAILED!");
            System.out.println("Unable to clean "+ DISPLAY_NAME);
        }
    }

    public boolean isRunning() {
        return bootupProcessExecutor.isProcessRunning(STORAGE_PIDFILE);
    }

    /**
     * Attempt to start Storage and perform periodic polling until it is ready. The readiness check is performed with nodetool.
     *
     * A {@link ProcessNotStartedException} will be thrown if Storage does not start after a timeout specified
     * in the 'WAIT_INTERVAL_SECOND' field.
     *
     * @throws ProcessNotStartedException
     */
    private void start() {
        // services/cassandra/cassandra -p <storage-pidfile> -l <storage-logdir>
        List<String> storageCmd = Arrays.asList(STORAGE_BIN.toString(), "-p", STORAGE_PIDFILE.toString(), "-l", getStorageLogPathFromGraknProperties().toAbsolutePath().toString());
        List<String> storageCmd_EscapeWhitespace = storageCmd.stream().map(string -> string.replace(" ", "\\ ")).collect(Collectors.toList());

        // services/cassandra/nodetool statusthrift 2>/dev/null | tr -d '\n\r'
        List<String> isStorageRunningCmd_EscapeWhitespace = Arrays.asList(bootupProcessExecutor.SH, "-c",
                NODETOOL_BIN.toString().replace(" ", "\\ ") + " statusthrift | tr -d '\n\r'");

        System.out.print("Starting " + DISPLAY_NAME + "...");
        System.out.flush();

        BootupProcessResult startStorage = bootupProcessExecutor.executeAndWait(storageCmd_EscapeWhitespace, graknHome.toFile());

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(STORAGE_STARTUP_TIMEOUT_SECOND);
        while(LocalDateTime.now().isBefore(timeout) && startStorage.exitCode() == 0) {
            System.out.print(".");
            System.out.flush();

            BootupProcessResult isStorageRunning = bootupProcessExecutor.executeAndWait(isStorageRunningCmd_EscapeWhitespace, graknHome.toFile());

            if(isStorageRunning.stdout().trim().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }

            try {
                Thread.sleep(bootupProcessExecutor.WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        String errorMessage = "Process exited with code " + startStorage.exitCode() + ": '" + startStorage.stderr() + "'";
        System.out.println("FAILED!");
        System.err.println("Unable to start " + DISPLAY_NAME + ". ");
        System.err.println(errorMessage);
        throw new ProcessNotStartedException();
    }

    private Path getStorageLogPathFromGraknProperties() {
        return Paths.get(graknProperties.getProperty(GraknConfigKey.LOG_DIR));
    }
}

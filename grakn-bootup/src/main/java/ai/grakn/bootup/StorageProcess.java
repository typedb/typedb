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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * A class responsible for managing the Storage process,
 * including starting, stopping, status checks, and cleaning the Storage data
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class StorageProcess extends AbstractProcessHandler {
    private static final String DISPLAY_NAME = "Storage";
    private static final String STORAGE_PROCESS_NAME = "CassandraDaemon";
    private static final long STORAGE_STARTUP_TIMEOUT_SECOND = 60;
    private static final Path STORAGE_PIDFILE = Paths.get(File.separator,"tmp","grakn-storage.pid");
    private static final Path STORAGE_BIN = Paths.get("services", "cassandra", "cassandra");
    private static final Path NODETOOL_BIN = Paths.get("services", "cassandra", "nodetool");
    private static final Path STORAGE_DATA = Paths.get("db", "cassandra");

    private final Path graknHome;
    private final GraknConfig graknProperties;

    public StorageProcess(Path graknHome, Path graknPropertiesPath) {
        this.graknHome = graknHome;
        this.graknProperties = GraknConfig.read(graknPropertiesPath.toFile());
    }

    public void startIfNotRunning() {
        boolean storageIsRunning = isProcessRunning(STORAGE_PIDFILE);
        if(storageIsRunning) {
            System.out.println(DISPLAY_NAME +" is already running");
        } else {
            start();
        }
    }

    private Path getStorageLogPathFromGraknProperties(){
        return Paths.get(graknProperties.getProperty(GraknConfigKey.LOG_DIR));
    }

    private void start() {
        System.out.print("Starting "+ DISPLAY_NAME +"...");
        System.out.flush();
        if(STORAGE_PIDFILE.toFile().exists()) {
            try {
                Files.delete(STORAGE_PIDFILE);
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        OutputCommand outputCommand = executeAndWait(new String[]{
                SH,
                "-c",
                graknHome.resolve(STORAGE_BIN)
                        + " -p " + STORAGE_PIDFILE
                        + " -l " + getStorageLogPathFromGraknProperties().toAbsolutePath()
        }, null, null);
        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(STORAGE_STARTUP_TIMEOUT_SECOND);

        while(LocalDateTime.now().isBefore(timeout) && outputCommand.exitStatus<1) {
            System.out.print(".");
            System.out.flush();

            OutputCommand storageStatus = executeAndWait(new String[]{
                    SH,
                    "-c",
                    graknHome.resolve(NODETOOL_BIN) + " statusthrift 2>/dev/null | tr -d '\n\r'"
            },null,null);
            if(storageStatus.output.trim().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_SECOND *1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("FAILED!");
        System.out.println("Unable to start "+ DISPLAY_NAME);
        throw new ProcessNotStartedException();
    }

    public void stop() {
        stopProgram(STORAGE_PIDFILE, DISPLAY_NAME);
    }

    public void status() {
        processStatus(STORAGE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME +" pid = '"+ getPidFromFile(STORAGE_PIDFILE).orElse("") +
                "' (from "+ STORAGE_PIDFILE +"), '"+ getPidFromPsOf(STORAGE_PROCESS_NAME) +"' (from ps -ef)");
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
        return isProcessRunning(STORAGE_PIDFILE);
    }
}

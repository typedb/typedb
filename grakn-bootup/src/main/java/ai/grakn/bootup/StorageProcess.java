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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.bootup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Comparator;

/**
 *
 * @author Michele Orsi
 */
public class StorageProcess extends AbstractProcessHandler {

    private static final String STORAGE_PROCESS_NAME = "CassandraDaemon";
    private static final Path STORAGE_PID = Paths.get(File.separator,"tmp","grakn-storage.pid");
    private static final long STORAGE_STARTUP_TIMEOUT_S=60;
    private static final String CASSANDRA = "cassandra";
    private static final String NAME = "Storage";

    private final Path homePath;

    public StorageProcess(Path homePath) {
        this.homePath = homePath;
    }

    public void start() {
        boolean storageIsRunning = processIsRunning(STORAGE_PID);
        if(storageIsRunning) {
            System.out.println(NAME+" is already running");
        } else {
            storageStartProcess();
        }
    }

    private void storageStartProcess() {
        StorageConfigProcessor.updateConfig();

        System.out.print("Starting "+NAME+"...");
        System.out.flush();
        if(STORAGE_PID.toFile().exists()) {
            try {
                Files.delete(STORAGE_PID);
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        OutputCommand outputCommand = executeAndWait(new String[]{
                SH,
                "-c",
                homePath.resolve(Paths.get("services", CASSANDRA, CASSANDRA)) + " -p " + STORAGE_PID
        }, null, null);
        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(STORAGE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout) && outputCommand.exitStatus<1) {
            System.out.print(".");
            System.out.flush();

            OutputCommand storageStatus = executeAndWait(new String[]{
                    SH,
                    "-c",
                    homePath + "/services/cassandra/nodetool statusthrift 2>/dev/null | tr -d '\n\r'"
            },null,null);
            if(storageStatus.output.trim().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S *1000);
            } catch (InterruptedException e) {
                // DO NOTHING
            }
        }
        System.out.println("FAILED!");
        System.out.println("Unable to start "+NAME);
        throw new ProcessNotStartedException();
    }

    public void stop() {
        stopProgram(STORAGE_PID,NAME);
    }

    public void status() {
        processStatus(STORAGE_PID, NAME);
    }

    public void statusVerbose() {
        System.out.println(NAME+" pid = '"+ getPidFromFile(STORAGE_PID).orElse("")+"' (from "+STORAGE_PID+"), '"+ getPidFromPsOf(STORAGE_PROCESS_NAME) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+NAME+"...");
        System.out.flush();
        try {
            Path dirPath = Paths.get("db", CASSANDRA);
            Files.walk( dirPath )
                    .map( Path::toFile )
                    .sorted( Comparator.comparing( File::isDirectory ) )
                    .forEach( File::delete );
            Files.createDirectories(homePath.resolve(Paths.get("db", CASSANDRA,"data")));
            Files.createDirectories(homePath.resolve(Paths.get("db", CASSANDRA,"commitlog")));
            Files.createDirectories(homePath.resolve(Paths.get("db", CASSANDRA,"saved_caches")));
            System.out.println("SUCCESS");
        } catch (IOException e) {
            System.out.println("FAILED!");
            System.out.println("Unable to clean "+NAME);
        }
    }

    public boolean isRunning() {
        return processIsRunning(STORAGE_PID);
    }
}

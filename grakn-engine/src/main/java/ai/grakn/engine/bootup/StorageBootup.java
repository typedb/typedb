/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.engine.bootup;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.engine.GraknConfig;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.FileUtils;
import org.janusgraph.diskstorage.cassandra.utils.CassandraDaemonWrapper;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.listener.ProcessListener;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.engine.bootup.BootupProcessExecutor.SH;
import static ai.grakn.engine.bootup.BootupProcessExecutor.WAIT_INTERVAL_SECOND;

/**
 * A class responsible for managing the bootup-related process for the Storage component, including
 * starting and stopping, performing status checks, and cleaning the data.
 * <p>
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
    private static final Path STORAGE_PIDFILE = Paths.get(System.getProperty("java.io.tmpdir"), "grakn-storage.pid");
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
        System.out.println(DISPLAY_NAME + " pid = '" + bootupProcessExecutor.getPidFromFile(STORAGE_PIDFILE).orElse("") +
                "' (from " + STORAGE_PIDFILE + "), '" + bootupProcessExecutor.getPidFromPsOf(STORAGE_PROCESS_NAME) + "' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning " + DISPLAY_NAME + "...");
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
            System.out.println("Unable to clean " + DISPLAY_NAME);
        }
    }

    public boolean isRunning() {
        return bootupProcessExecutor.isProcessRunning(STORAGE_PIDFILE);
    }

    /**
     * Attempt to start Storage and perform periodic polling until it is ready. The readiness check is performed with nodetool.
     * <p>
     * A {@link BootupException} will be thrown if Storage does not start after a timeout specified
     * in the 'WAIT_INTERVAL_SECOND' field.
     *
     * @throws BootupException
     */
    private void start(){

        System.out.print("Starting " + DISPLAY_NAME + "...");
        System.out.flush();
        Path graknHome = Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
        String cassandraConfig = graknHome.resolve("services").resolve("cassandra").resolve("cassandra.yaml").toString();
        if(isWindows())cassandraConfig="file:\\\\\\"+cassandraConfig;
        Path logback = graknHome.resolve("services").resolve("cassandra").resolve("logback.xml");
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        String classpath = graknHome.resolve("services").resolve("lib").toString()+File.separator+"*";

        Future<ProcessResult> result = null;
        try {
            result = new ProcessExecutor()
                    .readOutput(true)
                    .directory(graknHome.toFile())
                    .redirectError(stderr)
                    .addListener(new ProcessListener() {
                        @Override
                        public void afterStart(Process process, ProcessExecutor executor) {
                            super.afterStart(process, executor);
                            String pidString = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
                            try{
                                PrintWriter writer = new PrintWriter(STORAGE_PIDFILE.toString(), "UTF-8");
                                writer.println(pidString);
                                writer.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    })
                    .command("java", "-cp", classpath, "-Dcassandra.config=" + cassandraConfig, "-Dcassandra.jmx.local.port=7199", "-Dcassandra.logdir="+getStorageLogPathFromGraknProperties(), "-Dlogback.configurationFile="+logback, CassandraDaemon.class.getCanonicalName())
                    .start().getFuture();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(STORAGE_STARTUP_TIMEOUT_SECOND);

        while (LocalDateTime.now().isBefore(timeout) && !result.isDone() && stderr.toString().isEmpty()) {
            System.out.print(".");
            System.out.flush();


            String output = null;
            try {
                output = new ProcessExecutor().command("java", "-cp", classpath, "-Dlogback.configurationFile="+logback.toString(), org.apache.cassandra.tools.NodeTool.class.getCanonicalName(), "statusthrift")
                        .readOutput(true).execute()
                        .outputUTF8();
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }

            if (output.trim().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }

            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("FAILED!");
        System.err.println("Unable to start " + DISPLAY_NAME + ".");
        try {
            String errorMessage = "Process exited with code '" + result.get().getExitValue() + "': '" + stderr.toString() + "'";
            System.err.println(errorMessage);
            throw new BootupException();
        } catch (InterruptedException | ExecutionException e) {
            throw new BootupException(e);
        }

    }

    public static int exec(Class klass) throws IOException,
            InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className);

        Process process = builder.start();
        process.waitFor();
        return process.exitValue();
    }

    private Path getStorageLogPathFromGraknProperties() {
        return Paths.get(graknProperties.getProperty(GraknConfigKey.LOG_DIR));
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }
}

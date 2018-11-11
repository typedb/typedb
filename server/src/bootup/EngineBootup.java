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

package grakn.core.server.bootup;

import grakn.core.util.GraknConfigKey;
import grakn.core.util.GraknSystemProperty;
import grakn.core.util.GraknConfig;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static grakn.core.server.bootup.BootupProcessExecutor.WAIT_INTERVAL_SECOND;

/**
 * A class responsible for managing the Engine process,
 * including starting, stopping, and performing status checks
 *
 */
public class EngineBootup {
    private static final String DISPLAY_NAME = "Grakn Core Server";
    private static final long ENGINE_STARTUP_TIMEOUT_S = 300;
    private static final Path ENGINE_PIDFILE = Paths.get(System.getProperty("java.io.tmpdir"), "grakn-core-server.pid");
    private static final String JAVA_OPTS = GraknSystemProperty.ENGINE_JAVAOPTS.value();

    protected final Path graknHome;
    protected final Path graknPropertiesPath;
    private final GraknConfig graknProperties;

    private BootupProcessExecutor bootupProcessExecutor;

    public EngineBootup(BootupProcessExecutor bootupProcessExecutor, Path graknHome, Path graknPropertiesPath) {
        this.bootupProcessExecutor = bootupProcessExecutor;
        this.graknHome = graknHome;
        this.graknPropertiesPath = graknPropertiesPath;
        this.graknProperties = GraknConfig.read(graknPropertiesPath);
    }

    /**
     * @return the main class of Engine. In KGMS, this method will be overridden to return a different class.
     */
    public Class getEngineMainClass() {
        return Grakn.class;
    }

    public void startIfNotRunning(String benchmarkFlag) {
        boolean isEngineRunning = bootupProcessExecutor.isProcessRunning(ENGINE_PIDFILE);
        if (isEngineRunning) {
            System.out.println(DISPLAY_NAME + " is already running");
        } else {
            start(benchmarkFlag);
        }
    }

    public void stop() {
        bootupProcessExecutor.stopProcessIfRunning(ENGINE_PIDFILE, DISPLAY_NAME);
    }

    public void status() {
        bootupProcessExecutor.processStatus(ENGINE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME + " pid = '" + bootupProcessExecutor.getPidFromFile(ENGINE_PIDFILE).orElse("") + "' (from " + ENGINE_PIDFILE + "), '" + bootupProcessExecutor.getPidFromPsOf(getEngineMainClass().getName()) + "' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning " + DISPLAY_NAME + "...");
        System.out.flush();
        Path rootPath = graknHome.resolve("logs");
        try (Stream<Path> files = Files.walk(rootPath)) {
            files.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            Files.createDirectories(graknHome.resolve("logs"));
            System.out.println("SUCCESS");
        } catch (IOException e) {
            System.out.println("FAILED!");
            System.out.println("Unable to clean " + DISPLAY_NAME);
        }
    }

    public boolean isRunning() {
        return bootupProcessExecutor.isProcessRunning(ENGINE_PIDFILE);
    }

    private void start(String benchmarkFlag) {
        System.out.print("Starting " + DISPLAY_NAME + "...");
        System.out.flush();

        Future<BootupProcessResult> startEngineAsync = bootupProcessExecutor.executeAsync(engineCommand(benchmarkFlag), graknHome.toFile());

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(ENGINE_STARTUP_TIMEOUT_S);

        while (LocalDateTime.now().isBefore(timeout) && !startEngineAsync.isDone()) {
            System.out.print(".");
            System.out.flush();

            String host = graknProperties.getProperty(GraknConfigKey.SERVER_HOST_NAME);
            int port = graknProperties.getProperty(GraknConfigKey.GRPC_PORT);

            if (bootupProcessExecutor.isProcessRunning(ENGINE_PIDFILE) && isEngineReady(host, port)) {
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
            String errorMessage = "Process exited with code '" + startEngineAsync.get().exitCode() + "': '" + startEngineAsync.get().stderr() + "'";
            System.err.println(errorMessage);
            throw new BootupException(errorMessage);
        } catch (InterruptedException | ExecutionException e) {
            throw new BootupException(e);
        }
    }

    private List<String> engineCommand(String benchmarkFlag) {
        ArrayList<String> engineCommand = new ArrayList<>();
        engineCommand.add("java");
        engineCommand.add("-cp");
        engineCommand.add(getEngineClassPath());
        engineCommand.add("-Dgrakn.dir=" + graknHome);
        engineCommand.add("-Dgrakn.conf=" + graknPropertiesPath);
        engineCommand.add("-Dgrakn.pidfile=" + ENGINE_PIDFILE);
        // This is because https://wiki.apache.org/hadoop/WindowsProblems
        engineCommand.add("-Dhadoop.home.dir="+graknHome.resolve("services").resolve("hadoop"));
        if (JAVA_OPTS != null && JAVA_OPTS.length() > 0) {//split JAVA OPTS by space and add them to the command
            engineCommand.addAll(Arrays.asList(JAVA_OPTS.split(" ")));
        }
        engineCommand.add(getEngineMainClass().getName());

        // benchmarking flag
        engineCommand.add(benchmarkFlag);
        return engineCommand;
    }

    private String getEngineClassPath() {
        // TODO:
        // move server-build-bin_deploy.jar to the folder 'services/lib'
        // then, this code should be graknHome.resolve("services/lib/*.jar")
        Path jar = Paths.get("services", "lib", "server-binary_deploy.jar");
        return graknHome.resolve(jar) + File.pathSeparator + graknHome.resolve("conf");
    }

    private static boolean isEngineReady(String host, int port) {
        try {
            Socket s = new Socket(host, port);
            s.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}

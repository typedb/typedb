/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.tool.runner;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.tool.runner.Util.createProcessExecutor;
import static com.vaticle.typedb.core.tool.runner.Util.findUnusedPorts;
import static com.vaticle.typedb.core.tool.runner.Util.getServerArchiveFile;
import static com.vaticle.typedb.core.tool.runner.Util.typeDBCommand;
import static com.vaticle.typedb.core.tool.runner.Util.unarchive;

public class TypeDBCoreRunner implements TypeDBRunner {

    private final Path distribution;
    private final Path dataDir;
    private final Path logsDir;
    private final int port;
    private StartedProcess process;
    private final ProcessExecutor executor;
    private final Map<String, String> userOptions;

    private static final Map<String, String> STATIC_OPTIONS = map(
            pair("--development-mode.enable", "true")
    );

    public TypeDBCoreRunner() throws InterruptedException, TimeoutException, IOException {
        this(new HashMap<>());
    }

    public TypeDBCoreRunner(Map<String, String> userOptions) throws InterruptedException, TimeoutException, IOException {
        this.userOptions = userOptions;
        port = findUnusedPorts(1).get(0);
        System.out.println(address() + ": Constructing " + name() + " runner");
        System.out.println(address() + ": Extracting distribution archive...");
        distribution = unarchive(getServerArchiveFile());
        System.out.println(address() + ": Distribution archive extracted.");
        dataDir = distribution.resolve("server").resolve("data");
        logsDir = distribution.resolve("server").resolve("logs");
        executor = createProcessExecutor(distribution);
        System.out.println(address() + ": Runner constructed");
    }

    private String name() {
        return "TypeDB Core";
    }

    @Override
    public String address() {
        return host() + ":" + port();
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    @Override
    public void start() {
        System.out.println(address() + ": " +  name() + "is starting... ");
        System.out.println(address() + ": Distribution is located at " + distribution.toAbsolutePath());
        System.out.println(address() + ": Data directory is located at " + dataDir.toAbsolutePath());
        System.out.println(address() + ": Server bootup command = " + command());
        try {
            process = Util.startProcess(executor, command(), new InetSocketAddress(host(), port()));
        } catch (Throwable e) {
            printLogs();
            throw new RuntimeException(e);
        }
    }

    private List<String> command() {
        Map<String, String> dynamicOptions = map(
                pair("--server.address", address()),
                pair("--storage.data", dataDir.toAbsolutePath().toString())
        );

        List<String> cmd = new ArrayList<>();
        cmd.add("server");
        addOptions(STATIC_OPTIONS, cmd);
        addOptions(dynamicOptions, cmd);
        addOptions(userOptions, cmd);
        return typeDBCommand(cmd);
    }

    @Override
    public boolean isStopped() {
        return process == null || !process.getProcess().isAlive();
    }

    @Override
    public void stop() {
        if (process != null) {
            try {
                System.out.println(address() + ": Stopping...");
                CompletableFuture<Process> processFuture = process.getProcess().onExit();
                process.getProcess().destroy();
                processFuture.get();
                process = null;
                System.out.println(address() + ": Stopped.");
            } catch (Exception e) {
                System.out.println("Unable to destroy runner.");
                printLogs();
            }
        }
    }

    @Override
    public void deleteFiles() {
        stop();
        try {
            Util.deleteDirectoryContents(distribution);
        }
        catch (IOException e) {
            System.out.println("Unable to delete distribution " + distribution.toAbsolutePath());
            e.printStackTrace();
        }
    }

    @Override
    public void reset() {
        stop();
        List<Path> paths = Arrays.asList(dataDir, logsDir);
        paths.forEach(path -> {
            try {
                Util.deleteDirectoryContents(path);
            } catch (IOException e) {
                System.out.println("Unable to delete " + path.toAbsolutePath());
                e.printStackTrace();
            }
        });
    }

    private void addOptions(Map<String, String> options, List<String> outArgs) {
        options.forEach((key, value) -> outArgs.add(key + "=" + value));
    }

    private void printLogs() {
        System.out.println(address() + ": ================");
        System.out.println(address() + ": Logs:");
        Path logPath = logsDir.resolve("typedb.log").toAbsolutePath();
        try {
            executor.command("cat", logPath.toString()).execute();
        } catch (IOException | InterruptedException | TimeoutException e) {
            System.out.println(address() + ": Unable to print '" + logPath + "'");
            e.printStackTrace();
        }
        System.out.println(address() + ": ================");
    }
}

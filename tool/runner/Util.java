/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.tool.runner;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class Util {

    private static final String TAR_GZ = ".tar.gz";
    private static final String ZIP = ".zip";
    private static final int PORT_ALLOCATION_MAX_RETRIES = 15;
    public static final int SERVER_STARTUP_TIMEOUT_MILLIS = 30000;
    private static final int SERVER_ALIVE_POLL_INTERVAL_MILLIS = 500;
    private static final int SERVER_ALIVE_POLL_MAX_RETRIES = SERVER_STARTUP_TIMEOUT_MILLIS / SERVER_ALIVE_POLL_INTERVAL_MILLIS;

    public static File getServerArchiveFile() {
        String[] args = System.getProperty("sun.java.command").split(" ");
        Optional<CLIOptions> maybeOptions = CLIOptions.parseCLIOptions(args);
        if (!maybeOptions.isPresent()) {
            throw new IllegalArgumentException("No archives were passed as arguments");
        }
        CLIOptions options = maybeOptions.get();
        return new File(options.getServerArchive());
    }

    public static Path unarchive(File archive) throws IOException, TimeoutException, InterruptedException {
        Path runnerDir = Files.createTempDirectory("typedb");
        ProcessExecutor executor = createProcessExecutor(Paths.get(".").toAbsolutePath());
        if (archive.toString().endsWith(TAR_GZ)) {
            executor.command("tar", "-xf", archive.toString(),
                    "-C", runnerDir.toString()).execute();
        } else if (archive.toString().endsWith(ZIP)) {
            executor.command("unzip", "-q", archive.toString(),
                    "-d", runnerDir.toString()).execute();
        } else {
            throw new IllegalStateException(String.format("The distribution archive format must be either %s or %s", TAR_GZ, ZIP));
        }
        // The TypeDB Cloud archive extracts to a folder inside TYPEDB_TARGET_DIRECTORY named
        // typedb-server-{platform}-{version}. We know it's the only folder, so we can retrieve it using Files.list.
        return Files.list(runnerDir).findFirst().get().toAbsolutePath();
    }

    public static void deleteDirectoryContents(Path directory) throws IOException {
        Path finalDirectory = directory.toAbsolutePath();
        if (!finalDirectory.toFile().exists()) return;
        Files.walkFileTree(finalDirectory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (path.toFile().exists()) {
                    Files.delete(path);
                }
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e == null) {
                    if (dir.toFile().exists() && dir.toAbsolutePath() != finalDirectory.toAbsolutePath()) {
                        Files.delete(dir);
                    }
                } else {
                    throw e;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static List<String> typeDBCommand(String... cmd) {
        return typeDBCommand(Arrays.asList(cmd));
    }

    public static List<String> typeDBCommand(List<String> cmd) {
        List<String> command = new ArrayList<>();
        List<String> result;
        if (!System.getProperty("os.name").toLowerCase().contains("win")) {
            result = Collections.singletonList("typedb");
        } else {
            result = Arrays.asList("cmd.exe", "/c", "typedb.bat");
        }
        command.addAll(result);
        command.addAll(cmd);
        return command;
    }

    public static StartedProcess startProcess(ProcessExecutor executor, List<String> command, InetSocketAddress address) throws IOException, ExecutionException, InterruptedException {
        StartedProcess process = executor.command(command).start();
        boolean started = waitUntilPortUsed(address)
                .await(SERVER_STARTUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (!started) {
            String message = address + ": Unable to start. ";
            if (process.getFuture().isDone()) {
                ProcessResult processResult = process.getFuture().get();
                message += address + ": Process exited with code '" + processResult.getExitValue() + "'. ";
                if (processResult.hasOutput()) {
                    message += "Output: " + processResult.outputUTF8();
                }
            }
            throw new RuntimeException(message);
        } else {
            System.out.println(address + ": Started");
        }
        return process;
    }

    private static CountDownLatch waitUntilPortUsed(InetSocketAddress address) {
        CountDownLatch latch = new CountDownLatch(1);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            int retryNumber = 0;

            @Override
            public void run() {
                retryNumber++;
                if (retryNumber % 4 == 0) {
                    System.out.println(String.format("%s: waiting for server to start (%ds)...",
                            address.getHostString() + ":" + address.getPort(), retryNumber * SERVER_ALIVE_POLL_INTERVAL_MILLIS / 1000));
                }
                if (canConnectToServer()) {
                    latch.countDown();
                    timer.cancel();
                }
                if (retryNumber > SERVER_ALIVE_POLL_MAX_RETRIES) timer.cancel();
            }

            private boolean canConnectToServer() {
                try {
                    Socket s = new Socket(address.getHostString(), address.getPort());
                    s.close();
                    return true;
                } catch (IOException e) {
                    System.out.println(address.getHostString() + ":" + address.getPort() + ": Can't yet connect to server...");
                }
                return false;
            }
        }, 0, 500);
        return latch;
    }

    public static List<Integer> findUnusedPorts(int count) {
        assert count > 0;
        try {
            for (int retries = 0; retries < PORT_ALLOCATION_MAX_RETRIES; retries++) {
                List<Integer> ports = new ArrayList<>(count);
                // using port 0 automatically allocates a valid free port
                ServerSocket seed = new ServerSocket(0);
                ports.add(seed.getLocalPort());
                seed.close();
                for (int i = 1; i < count; i++) {
                    if (isPortUnused(ports.get(0) + i)) {
                        ports.add(ports.get(0) + i);
                    } else {
                        break;
                    }
                }
                if (ports.size() == count) {
                    // We sleep to make sure the ports are released correctly
                    // This is an alternative to using SO_REUSEADDR option for the server sockets, which ZMQ does not support
                    Thread.sleep(100);
                    return ports;
                }
            }
            throw new RuntimeException("Failed to allocate ports within  " + PORT_ALLOCATION_MAX_RETRIES + " retries");
        } catch (IOException e) {
            throw new RuntimeException("Error while searching for unused port.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isPortUnused(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static ProcessExecutor createProcessExecutor(Path directory) {
        return new ProcessExecutor()
                .directory(directory.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true)
                .environment("JAVA_HOME", System.getProperty("java.home"))
                .destroyOnExit();
    }

    @CommandLine.Command(name = "java")
    private static class CLIOptions {
        @CommandLine.Parameters String mainClass;
        @CommandLine.Option(
                names = {"--server"},
                description = "Location of the archive containing a server artifact."
        )
        private String serverArchive;

        public String getServerArchive() {
            return serverArchive;
        }

        public static Optional<CLIOptions> parseCLIOptions(String[] args) {
            CommandLine commandLine = new CommandLine(new CLIOptions()).setUnmatchedArgumentsAllowed(true);
            try {
                CommandLine.ParseResult result = commandLine.parseArgs(args);
                return Optional.of(result.asCommandLineList().get(0).getCommand());
            } catch (CommandLine.ParameterException ex) {
                commandLine.getErr().println(ex.getMessage());
                if (!CommandLine.UnmatchedArgumentException.printSuggestions(ex, commandLine.getErr())) {
                    ex.getCommandLine().usage(commandLine.getErr());
                }
                return Optional.empty();
            }
        }
    }
}

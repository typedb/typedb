/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.assembly;

import com.vaticle.typedb.console.tool.runner.TypeDBConsoleRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class DockerTest {
    private static final int ATTEMPTS = 25;
    private static final int ATTEMPT_SLEEP_MILLIS = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(DockerTest.class);
    private final ProcessExecutor executor;
    private static final int typeDBPort = 1729;

    public DockerTest() {
        executor = new ProcessExecutor().readOutput(true);
    }

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        String imagePath = Paths.get("assemble-docker-x86_64.tar").toAbsolutePath().toString();
        ProcessResult result = execute("docker", "load", "-i", imagePath);
        LOG.info(result.outputString());
        StartedProcess typeDBProcess = executor.command(
                "docker", "run", "--name", "typedb",
                "--rm", "-t", "-p", String.format("%d:%d", typeDBPort, typeDBPort),
                "bazel:assemble-docker",
                "/opt/typedb-all-linux-x86_64/typedb", "server", "--development-mode.enable=true"
        ).start();
        TypeDBConsoleRunner consoleRunner = new TypeDBConsoleRunner();
        testIsReady(consoleRunner);
        typeDBProcess.getProcess().destroy();
    }

    private void testIsReady(TypeDBConsoleRunner consoleRunner) throws InterruptedException {
        int attempt = 0;
        while (!isTypeDBServerReady(consoleRunner) && attempt < ATTEMPTS) {
            Thread.sleep(ATTEMPT_SLEEP_MILLIS);
            attempt++;
        }
        if (attempt == ATTEMPTS) {
            throw new RuntimeException("TypeDB server wasn't reachable in the allocated time of '" + (ATTEMPTS * ATTEMPT_SLEEP_MILLIS) + "' milliseconds.");
        }
    }

    private static boolean isTypeDBServerReady(TypeDBConsoleRunner consoleRunner) {
        return consoleRunner.run("--core", "localhost:" + typeDBPort, "--command", "database create docker-test") == 0;
    }

    private ProcessResult execute(String... cmd) throws InterruptedException, TimeoutException, IOException {
        ProcessResult result = executor.command(cmd).redirectError(System.err).redirectOutput(System.out).execute();
        if (result.getExitValue() != 0) {
            LOG.error("An error has occurred.");
            LOG.error(" - cmd: " + Arrays.toString(cmd));
            LOG.error(" - output: " + result.outputString());
            throw new RuntimeException();
        } else return result;
    }
}

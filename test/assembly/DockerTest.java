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

package com.vaticle.typedb.core.test.assembly;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class DockerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DockerTest.class);
    private final ProcessExecutor executor;
    private static final int typeDBPort = 1729;

    public DockerTest() {
        executor = new ProcessExecutor().readOutput(true);
    }

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        String imagePath = Paths.get("assemble-docker.tar").toAbsolutePath().toString();
        ProcessResult result = execute("docker", "load", "-i", imagePath);
        LOG.info(result.outputString());
        StartedProcess typeDBProcess = executor.command(
                "docker", "run", "--name", "typedb",
                "--rm", "-t", "-p", String.format("%d:%d", typeDBPort, typeDBPort),
                "bazel:assemble-docker"
        ).start();
        waitUntilReady();
        assertTrue("TypeDB failed to start", typeDBProcess.getProcess().isAlive());
        typeDBProcess.getProcess().destroy();
    }

    private void waitUntilReady() throws InterruptedException {
        int attempt = 0;
        while (!isTypeDBServerReady() && attempt < 25) {
            Thread.sleep(1000);
            attempt++;
        }
        if (!isTypeDBServerReady()) {
            throw new RuntimeException("TypeDB server didn't boot up in the allotted time");
        }
    }

    private static boolean isTypeDBServerReady() {
        try {
            Socket s = new Socket("localhost", typeDBPort);
            s.close();
            return true;
        } catch (IOException e) {
            return false;
        }
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

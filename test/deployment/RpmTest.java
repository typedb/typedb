/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.deployment;

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

import static org.junit.Assert.assertTrue;

public class RpmTest {
    private static final Logger LOG = LoggerFactory.getLogger(grakn.core.test.deployment.RpmTest.class);
    private static final String rpmSnapshot = "https://repo.grakn.ai/repository/meta/rpm-snapshot.repo";
    private static final int graknPort = 1729;

    private final String commit;
    private final ProcessExecutor executor;
    private StartedProcess graknProcess;

    public RpmTest() {
        commit = System.getenv("GRABL_COMMIT");
        executor = new ProcessExecutor().directory(Paths.get(".").toFile()).readOutput(true);
    }

    @Test
    public void test() throws InterruptedException, IOException, TimeoutException {
        setup();
        install();
        start();
        stop();
    }

    private void setup() throws InterruptedException, TimeoutException, IOException {
        execute("sudo", "yum-config-manager", "--add-repo", rpmSnapshot);
        execute("sudo", "yum", "update", "-y");
    }

    private void install() throws InterruptedException, TimeoutException, IOException {
        execute("sudo", "yum", "install", "-y", "grakn-core-server-0.0.0_" + commit);
    }

    private void start() throws InterruptedException, IOException, TimeoutException {
        graknProcess = executor.command("grakn", "server").start();

        waitUntilReady();
        assertTrue("Grakn Core failed to start", graknProcess.getProcess().isAlive());

        System.out.println("Grakn Core database server started");
    }

    private void waitUntilReady() throws InterruptedException, TimeoutException, IOException {
        int attempt = 0;
        while (!isGraknServerReady() && attempt < 25) {
            Thread.sleep(1000);
            attempt++;
        }
    }

    private void stop() {
        if (graknProcess != null) {
            try {
                System.out.println("Stopping Grakn Core database server");

                graknProcess.getProcess().destroy();

                System.out.println("Grakn Core database server stopped");
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private ProcessResult execute(String... cmd) throws InterruptedException, TimeoutException, IOException {
        ProcessResult result = executor.command(cmd).execute();
        if (result.getExitValue() != 0) {
            LOG.error("An error has occurred.");
            LOG.error(" - cmd: " + Arrays.toString(cmd));
            LOG.error(" - output: " + result.outputString());
            throw new RuntimeException();
        } else return result;
    }

    private boolean isGraknServerReady() throws TimeoutException, InterruptedException, IOException {
        int curlPortClosedExitCode = 7;
        ProcessResult result = executor.command("curl", "localhost:" + graknPort).execute();
        return result.getExitValue() != curlPortClosedExitCode;
    }
}

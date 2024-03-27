/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.deployment;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class AptTest {
    private static final Logger LOG = LoggerFactory.getLogger(AptTest.class);
    private static final String aptSnapshot = "https://repo.typedb.com/public/public-snapshot/deb/ubuntu";
    private static final String aptRelease = "https://repo.typedb.com/public/public-release/deb/ubuntu";
    private static final String pubkey1 = "17507562824cfdcc";
    private static final String pubkey2 = "https://cli-assets.heroku.com/apt/release.key";
    private static final String pubkey3 = "https://dl.google.com/linux/linux_signing_key.pub";
    private static final Path versionFile = Paths.get("VERSION");
    private static final int typeDBPort = 1729;

    private final String commit;
    private final ProcessExecutor executor;
    private StartedProcess typeDBProcess;

    public AptTest() {
        commit = System.getenv("TEST_DEPLOYMENT_APT_COMMIT");
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
        execute("sudo", "apt-key", "adv", "--keyserver", "keyserver.ubuntu.com", "--recv", pubkey1);
        execute("sudo", "add-apt-repository", "deb " + aptSnapshot + " trusty main");
        execute("sudo", "add-apt-repository", "deb " + aptRelease + " trusty main");
        execute("bash", "-c", "curl -L " + pubkey2 + " | sudo apt-key add -");
        execute("bash", "-c", "wget -q -O - " + pubkey3 + " | sudo apt-key add -");
        execute("sudo", "apt", "update");
    }

    private void install() throws InterruptedException, TimeoutException, IOException {
        System.out.println("core = " + commit);
        Files.writeString(versionFile, commit, StandardCharsets.US_ASCII);
        execute("sudo", "apt", "install", "-y", "typedb=0.0.0-" + commit);
    }

    private void start() throws InterruptedException, IOException {
        typeDBProcess = executor.command("typedb", "server", "--diagnostics.reporting.errors=false").start();

        waitUntilReady();
        assertTrue("TypeDB failed to start", typeDBProcess.getProcess().isAlive());

        System.out.println("TypeDB server started");
    }

    private void stop() {
        if (typeDBProcess != null) {
            System.out.println("Stopping TypeDB server");
            typeDBProcess.getProcess().destroy();
            System.out.println("TypeDB server stopped");
        }
    }

    private void waitUntilReady() throws InterruptedException {
        for (int attempt = 0; !isTypeDBServerReady() && attempt < 25; attempt++) {
            Thread.sleep(1000);
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
        ProcessResult result = executor.command(cmd).execute();
        if (result.getExitValue() != 0) {
            LOG.error("An error has occurred.");
            LOG.error(" - cmd: " + Arrays.toString(cmd));
            LOG.error(" - output: " + result.outputString());
            throw new RuntimeException();
        } else return result;
    }
}

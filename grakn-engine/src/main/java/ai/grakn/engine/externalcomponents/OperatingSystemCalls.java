/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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


package ai.grakn.engine.externalcomponents;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Responsible for interacting with the OS
 *
 * @author Ganeshwara Herawan Hananda
 */


public class OperatingSystemCalls {
    public boolean fileExists(String path) {
        return Files.exists(Paths.get(path));
    }

    public int psP(int pid) throws IOException, InterruptedException {
        Process ps = Runtime.getRuntime().exec(new String[]{"sh", "-c", " ps -p " + pid});

        return ps.waitFor();
    }

    public int catPidFile(String file) throws IOException {
        Process catProcess = Runtime.getRuntime().exec(new String[]{"sh", "-c", "cat " + file});
        String lines = readStdoutFromProcess(catProcess);
        return Integer.parseInt(lines);
    }

    public String readStdoutFromProcess(Process process) throws IOException {
        try (BufferedReader catStdout =
                 new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            return catStdout.lines().collect(Collectors.joining());
        }
    }

    public Process exec(String[] args) throws IOException {
        return Runtime.getRuntime().exec(args);
    }

    public int execAndReturn(String[] args) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(args);
        return process.waitFor();
    }

    public boolean isMac() {
        String OS = System.getProperty("os.name").toLowerCase();
        return OS.contains("mac");
    }
}

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

package ai.grakn.dist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 *
 * @author Michele Orsi
 */
public class ProcessHandler {

    OutputCommand executeAndWait(String[] cmdarray, String[] envp, File dir) {
        System.out.println(Arrays.toString(cmdarray));

        StringBuffer outputS = new StringBuffer();
        int exitValue = 1;

        Process p;
        BufferedReader reader = null;
        try {
            p = Runtime.getRuntime().exec(cmdarray, envp, dir);
            p.waitFor();
            exitValue = p.exitValue();
            reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));

            String line = "";
            while ((line = reader.readLine()) != null) {
                outputS.append(line + "\n");
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        OutputCommand outputCommand = new OutputCommand(outputS.toString().trim(), exitValue);
        return outputCommand;
    }

    Optional<String> getPidFromFile(Path fileName) {
        String pid=null;
        if (Files.exists(fileName)) {
            try {
                pid = new String(Files.readAllBytes(fileName),StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        return Optional.ofNullable(pid);
    }

    String getPidFromPsOf(String processName) {
        return executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "ps -ef | grep " + processName + " | grep -v grep | awk '{print $2}' "
            }, null, null).output;
    }

    void kill(String pid) {
        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "kill " + pid
        }, null, null);
    }

    String getClassPathFrom(Path home){
        FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
        File folder = new File(home + File.separator+"services"+File.separator+"lib"); // services/lib folder
        File[] values = folder.listFiles(jarFiles);
        if(values==null) {
            throw new RuntimeException("No libraries found: cannot run Grakn");
        }
        Stream<File> jars = Stream.of(values);
        File conf = new File(home + File.separator+"conf"+File.separator); // /conf
        File graknLogback = new File(home + File.separator+"services"+File.separator+"grakn"+File.separator); // services/grakn lib
        return ":"+Stream.concat(jars, Stream.of(conf, graknLogback))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .sorted() // we need to sort otherwise it doesn't load logback configuration properly
                .collect(joining(":"));
    }
}

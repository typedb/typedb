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

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.Grakn;
import ai.grakn.engine.GraknConfig;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Michele Orsi
 */
public class GraknProcess extends AbstractProcessHandler implements ProcessHandler {

    private Path homePath;
    private Path configPath;
    private GraknConfig graknConfig;

    private static final long GRAKN_STARTUP_TIMEOUT_S = 120;
    private static final Path GRAKN_PID = Paths.get(File.separator,"tmp","grakn.pid");

    public GraknProcess(Path homePath, Path configPath) {
        this.homePath = homePath;
        this.configPath = configPath;
        this.graknConfig = GraknConfig.read(configPath.toFile());
    }

    protected Class graknClass() {
        return Grakn.class;
    }

    public void start() {
        boolean graknIsRunning = processIsRunning(GRAKN_PID);
        if(graknIsRunning) {
            System.out.println(graknClass().getSimpleName()+" is already running");
        } else {
            graknStartProcess();
        }
    }

    private String getClassPathFrom(Path home){
        FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
        File folder = new File(home + File.separator+"services"+File.separator+"lib"); // services/lib folder
        File[] values = folder.listFiles(jarFiles);
        if(values==null) {
            throw new RuntimeException("No libraries found: cannot run "+graknClass().getSimpleName());
        }
        Stream<File> jars = Stream.of(values);
        File conf = new File(home + File.separator+"conf"+File.separator); // /conf
        File graknLogback = new File(home + File.separator+"services"+File.separator+"grakn"+File.separator); // services/grakn lib
        return ":"+Stream.concat(jars, Stream.of(conf, graknLogback))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .sorted() // we need to sort otherwise it doesn't load logback configuration properly
                .collect(Collectors.joining(":"));
    }

    private void graknStartProcess() {
        System.out.print("Starting "+graknClass().getSimpleName()+"...");
        System.out.flush();
        if(Files.exists(GRAKN_PID)) {
            try {
                Files.delete(GRAKN_PID);
            } catch (IOException e) {
                // DO NOTHING
            }
        }

        String command = "java -cp " + getClassPathFrom(homePath) + " -Dgrakn.dir=" + homePath + " -Dgrakn.conf="+ configPath +" "+graknClass().getName()+" > /dev/null 2>&1 &";
        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                command}, null, null);

        String pid = getPidFromPsOf(Grakn.class.getName());

        try {
            Files.write(GRAKN_PID,pid.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.out.println("Cannot write "+graknClass().getSimpleName()+" PID on a file");
        }

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(GRAKN_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            String host = graknConfig.getProperty(GraknConfigKey.SERVER_HOST_NAME);
            int port = graknConfig.getProperty(GraknConfigKey.SERVER_PORT);

            if(processIsRunning(GRAKN_PID) && graknCheckIfReady(host,port, REST.WebPath.STATUS)) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                // DO NOTHING
            }
        }

        if(Files.exists(GRAKN_PID)) {
            try {
                Files.delete(GRAKN_PID);
            } catch (IOException e) {
                // DO NOTHING
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start "+graknClass().getSimpleName());
        throw new ProcessNotStartedException();
    }

    private boolean graknCheckIfReady(String host, int port, String path) {
        try {
            URL siteURL = UriBuilder.fromUri(new SimpleURI(host, port).toURI()).path(path).build().toURL();
            HttpURLConnection connection = (HttpURLConnection) siteURL
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            int code = connection.getResponseCode();
            if (code == 200) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
    }

    public void stop() {
        stopProgram(GRAKN_PID,graknClass().getSimpleName());
    }

    public void status() {
        processStatus(GRAKN_PID, graknClass().getSimpleName());
    }

    public void statusVerbose() {
        System.out.println(graknClass().getSimpleName()+" pid = '"+ getPidFromFile(GRAKN_PID).orElse("")+"' (from "+GRAKN_PID+"), '"+ getPidFromPsOf(Grakn.class.getSimpleName()) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+graknClass().getSimpleName()+"...");
        System.out.flush();
        try {
            Files.delete(Paths.get(homePath +"logs"));
            Files.createDirectories(Paths.get(homePath +"logs"));
            System.out.println("SUCCESS");
        } catch (IOException e) {
            System.out.println("FAILED!");
            System.out.println("Unable to clean "+graknClass().getSimpleName());
        }
    }

    public boolean isRunning() {
        return processIsRunning(GRAKN_PID);
    }
}

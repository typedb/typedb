/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.bootup;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.bootup.graknengine.Grakn;
import ai.grakn.engine.GraknConfig;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class responsible for managing the Engine process,
 * including starting, stopping, and performing status checks
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class EngineProcess extends AbstractProcessHandler {
    private static final String DISPLAY_NAME = "Engine";
    private static final long ENGINE_STARTUP_TIMEOUT_S = 300;
    private static final Path ENGINE_PIDFILE = Paths.get(File.separator,"tmp","grakn-engine.pid");
    private static final String JAVA_OPTS = Optional.ofNullable(GraknSystemProperty.ENGINE_JAVAOPTS.value()).orElse("");
    private static Class ENGINE_MAIN_CLASS = Grakn.class;

    protected final Path graknHome;
    protected final Path graknPropertiesPath;
    private final GraknConfig graknProperties;

    public EngineProcess(Path graknHome, Path graknPropertiesPath) {
        this.graknHome = graknHome;
        this.graknPropertiesPath = graknPropertiesPath;
        this.graknProperties = GraknConfig.read(graknPropertiesPath.toFile());
    }

    public void startIfNotRunning() {
        boolean graknIsRunning = isProcessRunning(ENGINE_PIDFILE);
        if(graknIsRunning) {
            System.out.println(DISPLAY_NAME + " is already running");
        } else {
            start();
        }
    }

    protected String getClassPathFrom(Path home){
        FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
        File folder = new File(home + File.separator+"services"+File.separator+"lib"); // services/lib folder
        File[] values = folder.listFiles(jarFiles);
        if(values==null) {
            throw new RuntimeException("No libraries found: cannot run " + DISPLAY_NAME);
        }
        Stream<File> jars = Stream.of(values);
        File conf = home.resolve("conf").toFile(); // $GRAKN_HOME/conf
        File graknLogback = Paths.get(home.toAbsolutePath().toString(), "services", "grakn", "server").toFile(); // $GRAKN_HOME/services/grakn/server lib
        String classPath = ":"+Stream.concat(jars, Stream.of(conf, graknLogback))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .sorted() // we need to sort otherwise it doesn't load logback configuration properly
                .collect(Collectors.joining(":"));
        return classPath;
    }

    private void start() {
        System.out.print("Starting " + DISPLAY_NAME + "...");
        System.out.flush();

        String command = commandToRun();
        executeAndWait(new String[]{ "/bin/sh", "-c", command }, null, null);

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(ENGINE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            String host = graknProperties.getProperty(GraknConfigKey.SERVER_HOST_NAME);
            int port = graknProperties.getProperty(GraknConfigKey.SERVER_PORT);

            if(isProcessRunning(ENGINE_PIDFILE) && graknCheckIfReady(host,port, REST.WebPath.STATUS)) {
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
        System.out.println("Unable to start " + DISPLAY_NAME);
        throw new ProcessNotStartedException();
    }

    protected String commandToRun() {
        String cmd = "java " + JAVA_OPTS + " -cp " + getClassPathFrom(graknHome) + " -Dgrakn.dir=" + graknHome +
                " -Dgrakn.conf="+ graknPropertiesPath + " -Dgrakn.pidfile=" + ENGINE_PIDFILE.toString() + " " + ENGINE_MAIN_CLASS.getName() + " > /dev/null 2>&1 &";

        return cmd;
    }

    private boolean graknCheckIfReady(String host, int port, String path) {
        try {
            URL siteURL = UriBuilder.fromUri(new SimpleURI(host, port).toURI()).path(path).build().toURL();
            HttpURLConnection connection = (HttpURLConnection) siteURL
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            int code = connection.getResponseCode();
            return code == 200;
        } catch (IOException e) {
            return false;
        }
    }

    public void stop() {
        stopProgram(ENGINE_PIDFILE, DISPLAY_NAME);
    }

    public void status() {
        processStatus(ENGINE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME + " pid = '"+ getPidFromFile(ENGINE_PIDFILE).orElse("")+"' (from "+ ENGINE_PIDFILE +"), '"+ getPidFromPsOf(ENGINE_MAIN_CLASS.getName()) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+DISPLAY_NAME+"...");
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
            System.out.println("Unable to clean "+DISPLAY_NAME);
        }
    }

    public boolean isRunning() {
        return isProcessRunning(ENGINE_PIDFILE);
    }
}

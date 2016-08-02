/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.api.ImportController;
import io.mindmaps.api.RemoteShellController;
import io.mindmaps.api.VisualiserController;
import io.mindmaps.util.ConfigProperties;
import spark.Spark;

import java.util.Properties;

import static spark.Spark.port;

public class MindmapsEngineServer {

    public static void main(String[] args) {

        // --- Spark JAVA configurations ---- //

        //Spark.staticFileLocation("/public");

        // Max number of concurrent threads
        //  int maxThreads = 8;
        //  threadPool(maxThreads);

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);


        ConfigProperties prop = ConfigProperties.getInstance();

        // Listening port
        port(prop.getPropertyAsInt(ConfigProperties.SERVER_PORT_NUMBER));

        // --------------------------------- //


        // ----- APIs --------- //

        new RemoteShellController();
        new VisualiserController();
        new GraphFactoryController();
        new ImportController();

        // ------ WEB INTERFACE ----- //
//        new Dashboard();
//        new Visualiser();
//        new GraqlShell();
//        new Import();
    }
}

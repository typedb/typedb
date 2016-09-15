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
package io.mindmaps.engine;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import io.mindmaps.engine.controller.*;
import io.mindmaps.engine.util.ConfigProperties;
import org.apache.commons.logging.Log;
import org.slf4j.LoggerFactory;
import spark.Spark;

import static spark.Spark.*;

public class MindmapsEngineServer {

    public static void main(String[] args) {
        start();
    }

    public static void start() {

        ConfigProperties prop = ConfigProperties.getInstance();

        // Set host name
        ipAddress(prop.getProperty(ConfigProperties.SERVER_HOST_NAME));

        // Set port
        port(prop.getPropertyAsInt(ConfigProperties.SERVER_PORT_NUMBER));

        // Set the static files folder
        staticFiles.externalLocation(prop.getPath(ConfigProperties.STATIC_FILES_PATH));

        // Start all the controllers
        new RemoteShellController();
        new VisualiserController();
        new GraphFactoryController();
        new ImportController();
        new CommitLogController();
        new TransactionController();
        new StatusController();

        // This method will block until all the controllers are ready to serve requests
        awaitInitialization();

        printStartMessage(prop.getProperty(ConfigProperties.SERVER_HOST_NAME),prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER));
    }


    private static void printStartMessage(String host, String port){

        System.out.print(
                "  __  __ _           _                           ____  ____  \n" +
                " |  \\/  (_)_ __   __| |_ __ ___   __ _ _ __  ___|  _ \\| __ ) \n" +
                " | |\\/| | | '_ \\ / _` | '_ ` _ \\ / _` | '_ \\/ __| | | |  _ \\ \n" +
                " | |  | | | | | | (_| | | | | | | (_| | |_) \\__ \\ |_| | |_) |\n" +
                " |_|  |_|_|_| |_|\\__,_|_| |_| |_|\\__,_| .__/|___/____/|____/ \n" +
                "                                      |_|                    \n\n");

        System.out.println("Mindmaps Engine is ready. Listening on [http://" + host + ":" + port +"]");
    }
}

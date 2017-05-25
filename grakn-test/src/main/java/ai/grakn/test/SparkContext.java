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
 *
 */

package ai.grakn.test;


import com.jayway.restassured.RestAssured;
import org.junit.rules.ExternalResource;
import spark.Service;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Consumer;

import static ai.grakn.engine.GraknEngineServer.configureSpark;

/**
 * Context that starts spark
 * @author Felix Chapman
 */
public class SparkContext extends ExternalResource {

    private final Consumer<Service> createControllers;
    private Service spark;
    private int port;

    private SparkContext(Consumer<Service> createControllers) {
        this.createControllers = createControllers;
        this.port = getEphemeralPort();
    }

    public static SparkContext withControllers(Consumer<Service> createControllers) {
        return new SparkContext(createControllers);
    }

    public SparkContext port(int port) {
        this.port = port;
        return this;
    }

    public int port() {
        return port;
    }

    public String uri() {
        return "localhost:" + port;
    }

    public void start() {
        spark = Service.ignite();
        configureSpark(spark, port);

        RestAssured.baseURI = "http://localhost:" + port;

        createControllers.accept(spark);

        spark.awaitInitialization();
    }

    public void stop() {
        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            } catch(IllegalStateException e){
                running = false;
            }
        }
    }

    @Override
    protected void before() throws Throwable {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    private static int getEphemeralPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

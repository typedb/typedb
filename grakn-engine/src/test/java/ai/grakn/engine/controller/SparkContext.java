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

package ai.grakn.engine.controller;


import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.util.SimpleURI;
import ai.grakn.util.GraknTestUtil;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import org.junit.rules.ExternalResource;
import spark.Service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static ai.grakn.engine.HttpHandler.configureSpark;

/**
 * Context that starts spark
 * @author Felix Chapman
 */
public class SparkContext extends ExternalResource {

    private final BiConsumer<Service, GraknEngineConfig> createControllers;
    private final GraknEngineConfig config = GraknEngineConfig.create();

    private Service spark;
    
    private SparkContext(BiConsumer<Service, GraknEngineConfig> createControllers) {
        this.createControllers = createControllers;

        int port = GraknTestUtil.getEphemeralPort();
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, port);

        if ("0.0.0.0".equals(config.getProperty(GraknConfigKey.SERVER_HOST_NAME))) {
            config.setConfigProperty(GraknConfigKey.SERVER_HOST_NAME, "localhost");
        }
    }

    private Service startSparkCopyOnNewPort() {
        Service spark = Service.ignite();

        String hostName = config.getProperty(GraknConfigKey.SERVER_HOST_NAME);

        configureSpark(spark, hostName, port(), config.getPath(GraknConfigKey.STATIC_FILES_PATH),
                        64);

        RestAssured.baseURI = "http://" + hostName + ":" + port();

        RestAssured.requestSpecification = new RequestSpecBuilder().build();

        return spark;
    }
    
    public static SparkContext withControllers(BiConsumer<Service, GraknEngineConfig> createControllers) {
        return new SparkContext(createControllers);
    }

    public static SparkContext withControllers(Consumer<Service> createControllers) {
        return new SparkContext((spark, config) -> createControllers.accept(spark));
    }

    public SparkContext port(int port) {
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, port);
        return this;
    }

    public SparkContext host(String host) {
        config.setConfigProperty(GraknConfigKey.SERVER_HOST_NAME, host);
        return this;
    }

    public int port() {
        return config.getProperty(GraknConfigKey.SERVER_PORT);
    }

    public SimpleURI uri() {
        return config.uri();
    }

    public GraknEngineConfig config() {
        return config;
    }

    public void start() {
        spark = startSparkCopyOnNewPort();

        createControllers.accept(spark, config);

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

}

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

package ai.grakn.engine.controller;


import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import ai.grakn.test.rule.CompositeTestRule;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import spark.Service;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static ai.grakn.engine.ServerHTTP.configureSpark;

/**
 * Context that starts spark
 * @author Felix Chapman
 */
public class SparkContext extends CompositeTestRule {

    private final BiConsumer<Service, GraknConfig> createControllers;
    private final GraknConfig config = GraknConfig.create();
    private final TemporaryFolder staticFiles = new TemporaryFolder();

    private Service spark;
    
    private SparkContext(BiConsumer<Service, GraknConfig> createControllers) {
        this.createControllers = createControllers;

        if ("0.0.0.0".equals(config.getProperty(GraknConfigKey.SERVER_HOST_NAME))) {
            config.setConfigProperty(GraknConfigKey.SERVER_HOST_NAME, "localhost");
        }

        config.setConfigProperty(GraknConfigKey.SERVER_PORT, 0);
    }

    private Service startSparkCopyOnNewPort() {
        Service spark = Service.ignite();

        String hostName = config.getProperty(GraknConfigKey.SERVER_HOST_NAME);

        if (config.getProperty(GraknConfigKey.SERVER_PORT) == 0) {
            GraknTestUtil.allocateSparkPort(config);
        }

        configureSpark(spark, hostName, port(), config.getPath(GraknConfigKey.STATIC_FILES_PATH), 64);
        spark.init();

        RestAssured.baseURI = "http://" + hostName + ":" + port();

        RestAssured.requestSpecification = new RequestSpecBuilder().build();

        return spark;
    }
    
    public static SparkContext withControllers(BiConsumer<Service, GraknConfig> createControllers) {
        return new SparkContext(createControllers);
    }

    public static SparkContext withControllers(HttpController ... controllers) {
        return new SparkContext((spark, config) -> {
            Stream.of(controllers).forEach(controller -> controller.start(spark));
        });
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

    public GraknConfig config() {
        return config;
    }

    public TemporaryFolder staticFiles() {
        return staticFiles;
    }

    public void start() {
        config.setConfigProperty(GraknConfigKey.STATIC_FILES_PATH, staticFiles.getRoot().toPath());

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
    protected List<TestRule> testRules() {
        return ImmutableList.of(staticFiles);
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

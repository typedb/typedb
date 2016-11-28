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

package ai.grakn.engine;

import ai.grakn.GraknGraph;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import com.jayway.restassured.RestAssured;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static java.util.stream.Collectors.joining;

public abstract class GraknEngineTestBase {

    private final Logger LOG = LoggerFactory.getLogger(GraknEngineTestBase.class);

    @BeforeClass
    public static void setupControllers() throws InterruptedException {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
        GraknEngineServer.start();
        Thread.sleep(5000);
    }

    @AfterClass
    public static void takeDownControllers() throws InterruptedException {
        GraknEngineServer.stop();
        Thread.sleep(10000);
    }

    protected String getPath(String file){
        return GraknEngineTestBase.class.getClassLoader().getResource(file).getPath();
    }

    protected String readFileAsString(String file){
        try {
            return Files.readAllLines(Paths.get(getPath(file)), StandardCharsets.UTF_8)
                    .stream().collect(joining("\n"));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    protected void loadOntology(String file, String graphName) {
        LOG.debug("Loading new ontology .. ");

        try(GraknGraph graph = GraphFactory.getInstance().getGraph(graphName)) {

            String ontology = readFileAsString(file);

            graph.graql().parse(ontology).execute();
            graph.commit();

        } catch (GraknValidationException e){
            throw new RuntimeException(e);
        }

        LOG.debug("Ontology loaded. ");
    }
}

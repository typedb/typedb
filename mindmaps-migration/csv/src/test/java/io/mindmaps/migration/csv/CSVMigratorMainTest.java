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

package io.mindmaps.migration.csv;

import com.google.common.io.Files;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Graql;
import org.junit.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.ExpectedException;
import spark.utils.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;

public class CSVMigratorMainTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    private MindmapsGraph graph;

    private final String dataFile = getFile("pets/data/pets.csv").getAbsolutePath();;
    private final String templateFile = getFile("pets/template.gql").getAbsolutePath();

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup(){
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        load(getFile("pets/schema.gql"));

        exit.expectSystemExitWithStatus(0);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void csvMainTest(){
        runAndAssertDataCorrect(new String[]{"-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void tsvMainTest(){
        String tsvFile = getFile("pets/data/pets.tsv").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"-input", tsvFile, "-template", templateFile, "-separator", "\t", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void spacesMainTest(){
        String tsvFile = getFile("pets/data/pets.spaces").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"-input", tsvFile, "-template", templateFile, "-separator", " ", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainTestDistributedLoader(){
        runAndAssertDataCorrect(new String[]{"csv", "-input", dataFile, "-template", templateFile, "-uri", "localhost:4567", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainDifferentBatchSizeTest(){
        runAndAssertDataCorrect(new String[]{"-input", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainNoArgsTest(){
        exit.expectSystemExitWithStatus(1);
        runAndAssertDataCorrect(new String[]{});
    }

    @Test
    public void csvMainNoTemplateNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        runAndAssertDataCorrect(new String[]{"-input", dataFile});
    }

    @Test
    public void csvMainInvalidTemplateFileTest(){
        exception.expect(RuntimeException.class);
        runAndAssertDataCorrect(new String[]{"-input", dataFile + "wrong", "-template", templateFile + "wrong"});
    }

    @Test
    public void csvMainThrowableTest(){
        exception.expect(NumberFormatException.class);
        runAndAssertDataCorrect(new String[]{"-input", dataFile, "-template", templateFile, "-batch", "hello"});
    }

    @Test
    public void unknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        runAndAssertDataCorrect(new String[]{ "-whale", ""});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        // test
        Collection<Entity> pets = graph.getEntityType("pet").instances();
        assertEquals(9, pets.size());

        Collection<Entity> cats = graph.getEntityType("cat").instances();
        assertEquals(2, cats.size());

        Collection<Entity> hamsters = graph.getEntityType("hamster").instances();
        assertEquals(1, hamsters.size());

        // test empty value not created
        ResourceType<String> name = graph.getResourceType("name");
        ResourceType<String> death = graph.getResourceType("death");

        Entity puffball = graph.getResource("Puffball", name).ownerInstances().iterator().next().asEntity();
        assertEquals(0, puffball.resources(death).size());

        Entity bowser = graph.getResource("Bowser", name).ownerInstances().iterator().next().asEntity();
        assertEquals(1, bowser.resources(death).size());
    }

    private File getFile(String fileName){
        return new File(CSVMigratorMainTest.class.getClassLoader().getResource(fileName).getPath());
    }

    // common class
    private void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException|MindmapsValidationException e){
            throw new RuntimeException(e);
        }
    }
}

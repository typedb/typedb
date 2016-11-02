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
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;

public class CSVMigratorMainTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    private MindmapsGraph graph;

    private final String dataFile = getFile("single-file/data/cars.csv").getAbsolutePath();;
    private final String templateFile = getFile("single-file/template.gql").getAbsolutePath();

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
        load(getFile("single-file/schema.gql"));
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void csvMainTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void tsvMainTest(){
        String tsvFile = getFile("single-file/data/cars.tsv").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"-file", tsvFile, "-template", templateFile, "-delimiter", "\t", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainTestDistributedLoader(){
        runAndAssertDataCorrect(new String[]{"csv", "-file", dataFile, "-template", templateFile, "-uri", "localhost:4567", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainDifferentBatchSizeTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void csvMainNoFileNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Data file missing (-f)");
        runAndAssertDataCorrect(new String[]{});
    }

    @Test
    public void csvMainNoTemplateNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        runAndAssertDataCorrect(new String[]{"-file", dataFile});
    }

    @Test
    public void csvMainInvalidTemplateFileTest(){
        exception.expect(RuntimeException.class);
        runAndAssertDataCorrect(new String[]{"-file", dataFile + "wrong", "-template", templateFile + "wrong"});
    }

    @Test
    public void csvMainThrowableTest(){
        exception.expect(NumberFormatException.class);
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-batch", "hello"});
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
        Collection<Entity> makes = graph.getEntityType("make").instances();
        assertEquals(3, makes.size());

        Collection<Entity> models = graph.getEntityType("model").instances();
        assertEquals(4, models.size());

        // test empty value not created
        ResourceType description = graph.getResourceType("description");

        Entity venture = graph.getEntity("Venture");
        assertEquals(1, venture.resources(description).size());

        Entity ventureLarge = graph.getEntity("Venture Large");
        assertEquals(0, ventureLarge.resources(description).size());
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

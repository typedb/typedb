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

package io.mindmaps.test.migration.csv;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.migration.csv.Main;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.*;

import java.util.Collection;

import static junit.framework.TestCase.assertEquals;

public class CSVMigratorMainTest extends AbstractMindmapsMigratorTest {

    private final String dataFile = getFile("csv", "single-file/data/cars.csv").getAbsolutePath();;
    private final String templateFile = getFile("csv", "single-file/template.gql").getAbsolutePath();

    @Before
    public void setup(){
        load(getFile("csv", "single-file/schema.gql"));
    }

    @Test
    public void csvMainTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void tsvMainTest(){
        String tsvFile = getFile("csv", "single-file/data/cars.tsv").getAbsolutePath();
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
}

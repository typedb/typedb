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

package io.mindmaps.test.migration.json;

import io.mindmaps.concept.*;
import io.mindmaps.migration.json.Main;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.*;

import java.util.Collection;

import static junit.framework.TestCase.assertEquals;

public class JsonMigratorMainTest extends AbstractMindmapsMigratorTest {

    private final String dataFile = getFile("json", "simple-schema/data.json").getAbsolutePath();;
    private final String templateFile = getFile("json", "simple-schema/template.gql").getAbsolutePath();

    @Before
    public void setup(){
        load(getFile("json", "simple-schema/schema.gql"));
    }

    @Test
    public void jsonMigratorMainTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void jsonMainDistributedLoaderTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace(), "-uri", "localhost:4567"});
    }

    @Test
    public void jsonMainNoDataFileNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Data file missing (-f)");
        runAndAssertDataCorrect(new String[]{"json"});
    }

    @Test
    public void jsonMainNoTemplateFileNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        runAndAssertDataCorrect(new String[]{"-file", ""});
    }

    @Test
    public void jsonMainUnknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        runAndAssertDataCorrect(new String[]{"-whale", ""});
    }

    @Test
    public void jsonMainNoDataFileExistsTest(){
        exception.expect(RuntimeException.class);
        runAndAssertDataCorrect(new String[]{"-file", dataFile + "wrong", "-template", templateFile + "wrong"});
    }

    @Test
    public void jsonMainNoTemplateFileExistsTest(){
        exception.expect(RuntimeException.class);
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile + "wrong"});
    }

    @Test
    public void jsonMainBatchSizeArgumentTest(){
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace(),});
    }

    @Test
    public void jsonMainThrowableTest(){
        exception.expect(NumberFormatException.class);
        runAndAssertDataCorrect(new String[]{"-file", dataFile, "-template", templateFile, "-batch", "hello"});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();
        Entity address = getProperty(person, "has-address").asEntity();
        Entity streetAddress = getProperty(address, "address-has-street").asEntity();

        Resource number = getResource(streetAddress, "number").asResource();
        assertEquals(21L, number.getValue());

        Collection<Instance> phoneNumbers = getProperties(person, "has-phone");
        assertEquals(2, phoneNumbers.size());
    }
}

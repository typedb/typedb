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

package ai.grakn.test.migration.csv;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.load;

public class CSVMigratorMainTest {
    private GraknSession factory;
    private GraknGraph graph;

    private final String dataFile = getFile("csv", "pets/data/pets.csv").getAbsolutePath();
    private final String templateFile = getFile("csv", "pets/template.gql").getAbsolutePath();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setup(){
        factory = engine.factoryWithNewKeyspace();
        load(factory, getFile("csv", "pets/schema.gql"));
        graph = factory.open(GraknTxType.WRITE);
    }

    @Test
    public void csvMainTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace());
    }

    @Test
    public void tsvMainTest(){
        String tsvFile = getFile("csv", "pets/data/pets.tsv").getAbsolutePath();
        runAndAssertDataCorrect("-input", tsvFile, "-template", templateFile, "-separator", "\t", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void spacesMainTest(){
        String tsvFile = getFile("csv", "pets/data/pets.spaces").getAbsolutePath();
        runAndAssertDataCorrect("-input", tsvFile, "-template", templateFile, "-separator", " ", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void quoteMainTest(){
        String quoteFile = getFile("csv", "pets/data/pets.singlequotes").getAbsolutePath();
        runAndAssertDataCorrect("-input", quoteFile, "-template", templateFile, "-quote", "\'", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void nullMainTest(){
        String nullTemplate = getFile("csv", "pets/template-null.gql").getAbsolutePath();
        runAndAssertDataCorrect("-input", dataFile, "-template", nullTemplate, "-keyspace", graph.getKeyspace(), "-null", "");
    }

    @Test
    public void csvMainTestDistributedLoader(){
        runAndAssertDataCorrect("csv", "-input", dataFile, "-template", templateFile, "-uri", "localhost:4567", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainDifferentBatchSizeTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainDifferentNumActiveTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-a", "2", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainPropertiesTest(){
        graph.close();
        load(factory, getFile("csv", "multi-file/schema.gql"));
        String configurationFile = getFile("csv", "multi-file/migration.yaml").getAbsolutePath();
        run("csv", "-config", configurationFile, "-keyspace", graph.getKeyspace());
        graph = factory.open(GraknTxType.WRITE);
        assertPokemonGraphCorrect(graph);
    }

    @Test
    public void csvMainNoArgsTest(){
        run();
    }

    @Test
    public void csvMainNoTemplateNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        run("-input", dataFile);
    }

    @Test
    public void csvMainInvalidTemplateFileTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong");
    }

    @Test
    public void csvMainThrowableTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile, "-template", templateFile, "-batch", "hello");
    }

    @Test
    public void unknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        run("-whale", "");
    }

    private void run(String... args){
        CSVMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);
        if(graph.isClosed()) graph = factory.open(GraknTxType.WRITE); //Make sure the graph is open
        assertPetGraphCorrect(graph);
    }
}

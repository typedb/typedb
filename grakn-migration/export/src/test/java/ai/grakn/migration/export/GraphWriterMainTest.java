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

package ai.grakn.migration.export;

import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.example.PokemonGraphFactory;
import ai.grakn.factory.GraphFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class GraphWriterMainTest {

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        GraknGraph original = GraphFactory.getInstance().getGraph("original");
        PokemonGraphFactory.loadGraph(original);
        GraknEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        GraknEngineServer.stop();
    }

    @Test
    public void exportOntologyToSystemOutTest(){
        runAndAssertDataCorrect(new String[]{"export", "ontology", "-graph", "original"});
    }

    @Test
    public void exportDataToSystemOutTest(){
        runAndAssertDataCorrect(new String[]{"export", "data", "-graph", "original"});
    }

    @Test
    public void exportToFileTest(){
        runAndAssertDataCorrect(new String[]{"export", "data", "-file", "/tmp/pokemon.gql", "-graph", "original"});
        File pokemonFile = new File("/tmp/pokemon.gql");
        assertTrue(pokemonFile.exists());
    }

    @Test(expected = RuntimeException.class)
    public void exportToFileNotFoundTest(){
        runAndAssertDataCorrect(new String[]{"export", "data", "-file", "grah/?*", "-graph", "original"});
    }

    @Test(expected = RuntimeException.class)
    public void exportNoGraphNameTest(){
        runAndAssertDataCorrect(new String[]{"export", "ontology"});
    }

    @Test
    public void exportEngineURLProvidedTest(){
        runAndAssertDataCorrect(new String[]{"export", "data", "-engine", "0.0.0.0:4567", "-graph", "original"});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);
    }
}

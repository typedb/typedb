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

package ai.grakn.test.migration.export;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.migration.export.Main;
import ai.grakn.test.EngineContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class GraphWriterMainTest {

    @ClassRule
    public static final EngineContext engineContext = EngineContext.startInMemoryServer();

    private static String keyspace;

    @BeforeClass
    public static void loadMovieGraph() {
        GraknGraph graph = engineContext.factoryWithNewKeyspace().open(GraknTxType.WRITE);
        keyspace = graph.getKeyspace();
        graph.close();
    }

    @Test
    public void exportOntologyToSystemOutTest(){
        runAndAssertDataCorrect("export", "-ontology", "-keyspace", keyspace);
    }

    @Test
    public void exportDataToSystemOutTest(){
        runAndAssertDataCorrect("export", "-data", "-keyspace", keyspace);
    }
    
    @Test
    public void exportNoArgsTest(){
        runAndAssertDataCorrect("export", "ontology");
    }

    @Test
    public void exportOnlyHelpMessageTest(){
        runAndAssertDataCorrect("export", "-h");
    }

    @Test
    public void exportEngineURLProvidedTest(){
        runAndAssertDataCorrect("export", "-data", "-uri", "localhost:4567", "-keyspace", keyspace);
    }

    private void runAndAssertDataCorrect(String... args){
        Main.main(args);
    }
}

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
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.loader.Loader;
import ai.grakn.example.PokemonGraphFactory;
import ai.grakn.migration.export.Main;
import ai.grakn.test.EngineTestBase;
import ai.grakn.test.GraknTestEnv;
import ai.grakn.test.migration.AbstractGraknMigratorTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphWriterMainTest {
	private String keyspace;
	
	@BeforeClass
	public static void startup() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(Loader.class);
        logger.setLevel(Level.DEBUG);

        // We will be submitting tasks to Engine, and checking if they have completed.
        EngineTestBase.startTestEngine();		
	}

	@AfterClass
	public static void shutdown() throws Exception {
        EngineTestBase.stopTestEngine();		
	}
	
	@Before
	public void init() {
        if (GraknTestEnv.usingOrientDB()) {
            this.keyspace = "memory";
        } else {
            this.keyspace = "a"+UUID.randomUUID().toString().replaceAll("-", "");
        }		
	}
	
    @Test
    public void exportOntologyToSystemOutTest(){
        runAndAssertDataCorrect("export", "-ontology", "-keyspace", this.keyspace);
    }

    @Test
    public void exportDataToSystemOutTest(){
        runAndAssertDataCorrect("export", "-data", "-keyspace", this.keyspace);
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
        runAndAssertDataCorrect("export", "-data", "-uri", "localhost:4567", "-keyspace", this.keyspace);
    }

    private void runAndAssertDataCorrect(String... args){
        Main.main(args);
    }
}

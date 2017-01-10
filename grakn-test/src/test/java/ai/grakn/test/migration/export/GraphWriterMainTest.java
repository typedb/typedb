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

import ai.grakn.migration.export.Main;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.test.GraknTestEnv;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class GraphWriterMainTest extends AbstractGraphTest {
	private String keyspace;
	
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

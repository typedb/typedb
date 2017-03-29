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

package ai.grakn.dist;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertTrue;

public class ExamplesTest {
    private GraknGraph graph;

    @Before
    public void setUp() {
        graph = Grakn.session("in-memory", "my-pokemon-graph").open(GraknTxType.WRITE);
    }

    @After
    public void closeGraph(){
        graph.close();
    }

    @Test
    public void testModern() throws IOException {
        runInsertQuery("src/examples/modern.gql");
        assertTrue(graph.graql().match(var().has("name", "marko").isa("person")).ask().execute());
    }

    @Test
    public void testPokemon() throws IOException {
        runInsertQuery("src/examples/pokemon.gql");
        assertTrue(graph.graql().match(var().rel(var().has("name", "Pikachu")).rel(var().has("name", "electric")).isa("has-type")).ask().execute());
    }

    private void runInsertQuery(String path) throws IOException {
        String query = Files.readAllLines(Paths.get(path)).stream().collect(joining("\n"));
        graph.graql().parse(query).execute();
    }
}

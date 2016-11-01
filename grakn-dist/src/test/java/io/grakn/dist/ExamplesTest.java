/*
 * GraknDB - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Research Ltd
 *
 * GraknDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GraknDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GraknDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.dist;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.grakn.graql.Graql.id;
import static io.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertTrue;

public class ExamplesTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        GraknGraph graph = Grakn.factory("in-memory", "my-graph").getGraph();
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testModern() throws IOException {
        runInsertQuery("src/examples/modern.gql");
        assertTrue(qb.match(id("marko").isa("person")).ask().execute());
    }

    @Test
    public void testPhilosophers() throws IOException {
        runInsertQuery("src/examples/philosophers.gql");
        assertTrue(qb.match(id("Alexander").has("title", "Shah of Persia")).ask().execute());
    }

    @Test
    public void testPokemon() throws IOException {
        runInsertQuery("src/examples/pokemon.gql");
        assertTrue(qb.match(var().rel(id("Pikachu")).rel(id("electric")).isa("has-type")).ask().execute());
    }

    private void runInsertQuery(String path) throws IOException {
        String query = Files.readAllLines(Paths.get(path)).stream().collect(joining("\n"));
        qb.parse(query).execute();
    }
}

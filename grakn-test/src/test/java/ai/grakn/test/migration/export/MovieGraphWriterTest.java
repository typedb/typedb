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

import ai.grakn.example.MovieGraphFactory;
import org.junit.Before;
import org.junit.Test;

public class MovieGraphWriterTest extends GraphWriterTestBase {

    @Before
    public void setup() {
        MovieGraphFactory.loadGraph(graph);
    }

    @Test
    public void testWritingMovieGraphOntology() {
        String ontology = writer.dumpOntology();
        insert(copy, ontology);

        assertOntologiesEqual(graph, copy);
    }

    @Test
    public void testWritingMovieGraphData() {
        String ontology = writer.dumpOntology();
        insert(copy, ontology);

        String data = writer.dumpData();
        insert(copy, data);

        assertDataEqual(graph, copy);
    }
}

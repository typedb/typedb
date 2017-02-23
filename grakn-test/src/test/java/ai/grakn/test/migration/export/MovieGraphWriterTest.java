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

import ai.grakn.graphs.MovieGraph;
import ai.grakn.migration.export.GraphWriter;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.test.migration.export.GraphWriterTestUtil.assertDataEqual;
import static ai.grakn.test.migration.export.GraphWriterTestUtil.assertOntologiesEqual;
import static ai.grakn.test.migration.export.GraphWriterTestUtil.insert;

public class MovieGraphWriterTest {

    private GraphWriter writer;

    @ClassRule
    public static GraphContext original = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public GraphContext copy = GraphContext.empty();

    @Before
    public void setup() {
        writer = new GraphWriter(original.graph());
    }

    @Test
    public void testWritingMovieGraphOntology() {
        String ontology = writer.dumpOntology();
        insert(copy.graph(), ontology);

        assertOntologiesEqual(original.graph(), copy.graph());
    }

    @Test
    public void testWritingMovieGraphData() {
        String ontology = writer.dumpOntology();
        insert(copy.graph(), ontology);

        String data = writer.dumpData();
        insert(copy.graph(), data);

        assertDataEqual(original.graph(), copy.graph());
    }
}

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
package io.mindmaps.migration.export;

import io.mindmaps.Mindmaps;
import io.mindmaps.example.MovieGraphFactory;
import org.junit.Before;
import org.junit.Test;

public class MovieGraphWriterTest extends GraphWriterTestBase {

    @Before
    public void setup() {
        original = Mindmaps.factory(Mindmaps.IN_MEMORY, "original").getGraph();
        copy = Mindmaps.factory(Mindmaps.IN_MEMORY, "copy").getGraph();
        writer = new GraphWriter(original);

        MovieGraphFactory.loadGraph(original);
    }

    @Test
    public void testWritingMovieGraphOntology() {
        String ontology = writer.dumpOntology();
        copy.graql().parse(ontology).execute();

        assertOntologiesEqual(original, copy);
    }

    @Test
    public void testWritingMovieGraphData() {
        String ontology = writer.dumpOntology();
        System.out.println(ontology);
        copy.graql().parse(ontology).execute();

        String data = writer.dumpData();
        copy.graql().parse(data).execute();

        assertDataEqual(original, copy);
    }
}

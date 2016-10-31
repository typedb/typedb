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
package io.grakn.migration.export;

import io.grakn.example.MovieGraphFactory;
import io.grakn.graql.Graql;
import org.junit.BeforeClass;
import org.junit.Test;

public class MovieGraphWriterTest extends GraphWriterTestBase {

    @BeforeClass
    public static void setup() {
        MovieGraphFactory.loadGraph(original);
    }

    @Test
    public void testWritingMovieGraphOntology() {
        String ontology = writer.dumpOntology();
        Graql.withGraph(copy).parse(ontology).execute();

        assertOntologiesEqual(original, copy);
    }

    @Test
    public void testWritingMovieGraphData() {
        String ontology = writer.dumpOntology();
        Graql.withGraph(copy).parse(ontology).execute();

        String data = writer.dumpData();
        Graql.withGraph(copy).parse(data).execute();

        assertDataEqual(original, copy);
    }
}

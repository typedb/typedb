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

package io.mindmaps.graql.parser;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.admin.VarAdmin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static io.mindmaps.graql.Graql.parsePatterns;
import static org.junit.Assert.*;

public class QueryParserFragmentsTest {

    private static MindmapsGraph mindmapsGraph;
    private QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
    }

    @Before
    public void setUp() {
        qb = Graql.withGraph(mindmapsGraph);
    }

    @Test
    public void testParseInfinitePatternsStream() throws IOException {
        InputStream stream = new InfiniteStream("$x isa person; ($x, $y) isa has-cast;\n");

        Iterator<Pattern> patterns = parsePatterns(stream).iterator();

        VarAdmin var1 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var1.toString());

        VarAdmin var2 = patterns.next().admin().asVar();
        assertTrue(var2.isRelation());
        assertEquals(2, var2.getCastings().size());

        VarAdmin var3 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var3.toString());

        assertTrue(patterns.hasNext());
    }

    @Test
    public void testParseFinitePatternsStream() throws IOException {
        String query = "$x isa person; ($x, $y) isa has-cast;";
        InputStream stream = new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8));

        Iterator<Pattern> patterns = qb.parsePatterns(stream).iterator();

        VarAdmin var1 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var1.toString());

        VarAdmin var2 = patterns.next().admin().asVar();
        assertTrue(var2.isRelation());
        assertEquals(2, var2.getCastings().size());

        assertFalse(patterns.hasNext());
    }


    class InfiniteStream extends InputStream {

        final String string;
        InputStream stream;

        public InfiniteStream(String string) {
            this.string = string;
            resetStream();
        }

        private void resetStream() {
            stream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public int read() throws IOException {
            int next = stream.read();

            if (next == -1) {
                stream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
                return stream.read();
            } else {
                return next;
            }
        }
    }
}

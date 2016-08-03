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

package io.mindmaps.graql.api.parser;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryParserFragmentsTest {

    private static MindmapsTransaction transaction;
    private QueryParser qp;

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Before
    public void setUp() {
        qp = QueryParser.create(transaction);
    }

    @Test
    public void testParsePattern() {
        List<Pattern> patterns = qp.parsePatterns("$x isa person; ($x, $y) isa has-cast;");

        assertTrue(patterns.get(0).admin().isVar());
        assertTrue(patterns.get(1).admin().isVar());

        Var.Admin var1 = patterns.get(0).admin().asVar();
        Var.Admin var2 = patterns.get(1).admin().asVar();

        assertEquals("$x isa person", var1.toString());

        assertTrue(var2.isRelation());
        assertEquals(2, var2.getCastings().size());
    }

    @Test
    public void testParsePatternsStream() throws IOException {
        InputStream stream = new InfiniteStream("$x isa person; ($x, $y) isa has-cast;\n");

        Iterator<Pattern> patterns = qp.parsePatternsStream(stream).iterator();

        Var.Admin var1 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var1.toString());

        Var.Admin var2 = patterns.next().admin().asVar();
        assertTrue(var2.isRelation());
        assertEquals(2, var2.getCastings().size());

        Var.Admin var3 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var3.toString());

        assertTrue(patterns.hasNext());
    }


    class InfiniteStream extends InputStream {

        String string;
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

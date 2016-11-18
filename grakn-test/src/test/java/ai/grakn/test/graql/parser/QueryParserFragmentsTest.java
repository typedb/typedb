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

package ai.grakn.test.graql.parser;

import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.parser.QueryParser;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static ai.grakn.graql.Graql.parsePatterns;
import static ai.grakn.graql.Graql.withoutGraph;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class QueryParserFragmentsTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testParseInfinitePatternsStream() throws IOException {
        InputStream stream = new InfiniteStream("#TRAP COMMENT\ninsert", "$x isa person; ($x, $y) isa has-cast;\n");

        Iterator<Pattern> patterns = parsePatterns(stream).iterator();

        VarAdmin var1 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var1.toString());

        VarAdmin var2 = patterns.next().admin().asVar();
        assertTrue(var2.hasProperty(RelationProperty.class));
        assertEquals(2, var2.getProperty(RelationProperty.class).get().getRelationPlayers().count());

        VarAdmin var3 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var3.toString());

        assertTrue(patterns.hasNext());
    }

    @Test
    public void testParseFinitePatternsStream() throws IOException {
        String query = "#TRAP COMMENT\ninsert $x isa person; ($x, $y) isa has-cast;";
        InputStream stream = new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8));

        Iterator<Pattern> patterns = parsePatterns(stream).iterator();

        VarAdmin var1 = patterns.next().admin().asVar();
        assertEquals("$x isa person", var1.toString());

        VarAdmin var2 = patterns.next().admin().asVar();
        assertTrue(var2.hasProperty(RelationProperty.class));
        assertEquals(2, var2.getProperty(RelationProperty.class).get().getRelationPlayers().count());

        assertFalse(patterns.hasNext());
    }

    @Test
    public void testParseFinitePatternStreamWithSyntaxError() throws IOException {
        String query = "insert\n\n($x, $y) is has-cast";
        InputStream stream = new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8));

        Iterator<Pattern> patterns = parsePatterns(stream).iterator();

        // Expect no pointer to the line text, but the line number and error
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(containsString("3"), containsString("isa"), not(containsString("^"))));
        patterns.next().admin().asVar();
    }

    @Test
    public void testParseInfiniteMatchInsert() throws IOException {
        InputStream stream = new InfiniteStream("#TRAP COMMENT\n", "match $x isa person; insert ($x, $y) isa has-cast;");

        Iterator<Object> objects = QueryParser.create(withoutGraph()).parseBatchLoad(stream).iterator();

        assertEquals("match", objects.next());
        assertEquals("$x isa person", objects.next().toString());
        assertEquals("insert", objects.next());
        assertTrue(((VarAdmin) objects.next()).hasProperty(RelationProperty.class));
        assertEquals("match", objects.next());
        assertTrue(objects.hasNext());
    }

    class InfiniteStream extends InputStream {

        private final String string;
        InputStream stream;

        public InfiniteStream(String prefix, String string) {
            this.string = string;
            stream = new ByteArrayInputStream((prefix + string).getBytes(StandardCharsets.UTF_8));
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

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

import io.mindmaps.graql.GraqlShell;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraqlShellTest {

    private GraqlClientMock client;
    private String expectedVersion = "graql-9.9.9";

    @Before
    public void setUp() {
        client = new GraqlClientMock();
    }

    @Test
    public void testClosesGraph() throws IOException {
        testShell("exit");
        assertTrue(client.isClosed());
    }

    @Test
    public void testDefaultNamespace() throws IOException {
        testShell("");
        assertEquals(Optional.of("mindmaps"), client.getNamespace());
    }

    @Test
    public void testSpecifiedNamespace() throws IOException {
        testShell("", "-n", "myspace");
        assertEquals(Optional.of("myspace"), client.getNamespace());
    }

    private String testShell(String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream err, String... args) throws IOException {
        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream pout = new PrintStream(bout);
        PrintStream perr = new PrintStream(err);

        GraqlShell.runShell(args, expectedVersion, in, pout, perr, client);

        pout.flush();
        perr.flush();

        return bout.toString();
    }
}

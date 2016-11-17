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

package ai.grakn.test.graql.shell;

import ai.grakn.graql.GraqlClientImpl;
import ai.grakn.graql.GraqlShell;
import ai.grakn.test.AbstractGraphTest;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GraqlShellWriteTest extends AbstractGraphTest {
    private static String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    @After
    public void resetIO() {
        System.setIn(null);
        System.setOut(null);
        System.setErr(null);
    }

    @Test
    public void testSpecifiedKeyspace() throws Exception {
        testShell("", "-e", "insert movie isa entity-type;");
        assertNotNull(graph.getEntityType("movie"));
    }

    @Test
    public void fuzzTest() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            testShell(randomString(i), err);
        }
    }

    @Test
    public void testCommitError() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String out = testShell("insert bob isa relation-type;\ncommit;\nmatch $x isa relation-type;\n", err);
        assertFalse(out, err.toString().isEmpty());
    }

    @Test
    public void testCommitErrorExecuteOption() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String out = testShell("", err, "-e", "insert bob isa relation-type;");
        assertFalse(out, err.toString().isEmpty());
    }

    private static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        random.ints().limit(length).forEach(i -> sb.append((char) i));

        return sb.toString();
    }

    private String testShell(String input, String... args) throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String result = testShell(input, err, args);
        assertTrue(err.toString(), err.toString().isEmpty());
        return result;
    }

    private String testShell(String input, ByteArrayOutputStream berr, String... args) throws Exception {
        String[] newArgs = Arrays.copyOf(args, args.length + 2);

        newArgs[newArgs.length-2] = "-k";
        newArgs[newArgs.length-1] = graph.getKeyspace();

        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        PrintStream err = new PrintStream(berr);

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);
            
            GraqlShell.runShell(newArgs, expectedVersion, historyFile, new GraqlClientImpl());
        } catch (Exception e) {
            System.setErr(null);
            e.printStackTrace();
            err.flush();
            fail(berr.toString());
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();


        return bout.toString();
    }
}


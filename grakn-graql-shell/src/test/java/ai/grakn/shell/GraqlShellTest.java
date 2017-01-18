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

package ai.grakn.shell;

import ai.grakn.graql.GraqlShell;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

// TODO: Fix randomly failing tests
// GraqlShellTest sometimes stalls with no output. This is probably related to the mock websocket
@Ignore
public class GraqlShellTest {

    private InputStream trueIn;
    private PrintStream trueOut;
    private PrintStream trueErr;

    private GraqlClientMock client;
    private final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    @Before
    public void setUp() {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @After
    public void resetIO() {
        System.setIn(trueIn);
        System.setOut(trueOut);
        System.setErr(trueErr);
    }

    @Test
    public void testDefaultKeyspace() throws IOException {
        testShell("");
        assertEquals("grakn", client.getKeyspace());
    }

    @Test
    public void testSpecifiedKeyspace() throws IOException {
        testShell("", "-k", "myspace");
        assertEquals("myspace", client.getKeyspace());
    }

   @Test
    public void testDefaultUri() throws IOException {
        testShell("");
        assertEquals("ws://localhost:4567/shell/remote", client.getURI().toString());
    }

    @Test
    public void testSpecifiedUri() throws IOException {
        testShell("", "-u", "1.2.3.4:5678");
        assertEquals("ws://1.2.3.4:5678/shell/remote", client.getURI().toString());
    }

    private String testShell(String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream berr, String... args) throws IOException {
        client = new GraqlClientMock();

        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        PrintStream err = new PrintStream(berr);

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);

            GraqlShell.runShell(args, expectedVersion, historyFile, client);
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();

        return bout.toString();
    }
}

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
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GraqlShellTest {

    private GraqlClientMock client = new GraqlClientMock();
    private final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

   @Test
    public void testDefaultUri() throws IOException {
       GraqlShell.runShell(new String[]{}, expectedVersion, historyFile, client);
       assertEquals("ws://localhost:4567/shell/remote", client.getURI().toString());
    }

    @Test
    public void testSpecifiedUri() throws IOException {
        GraqlShell.runShell(new String[]{"-r", "1.2.3.4:5678"}, expectedVersion, historyFile, client);
        assertEquals("ws://1.2.3.4:5678/shell/remote", client.getURI().toString());
    }
}

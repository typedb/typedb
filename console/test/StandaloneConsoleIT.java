/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.console.test;

import grakn.core.console.GraknConsole;
import grakn.core.console.exception.GraknConsoleException;
import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Test console without starting the Grakn test server
 */
public class StandaloneConsoleIT {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void consoleThrowsWhenNoServerRunning() throws ParseException, IOException, InterruptedException {
        expectedException.expect(GraknConsoleException.class);
        expectedException.expectMessage("Unable to create connection to Grakn instance at");
        GraknConsole console = new GraknConsole(new String[0], null, null);
        console.run();
    }
}

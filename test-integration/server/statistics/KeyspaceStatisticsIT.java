/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.statistics;

import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KeyspaceStatisticsIT {

    private SessionImpl session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() { session.close(); }


    @Test
    public void newKeyspaceHasZeroCounts() {
        KeyspaceStatistics statistics = session.keyspaceStatistics();
        int entityCount = statistics.count("entity");
        int relationCount = statistics.count("relation");
        int attributeCount = statistics.count("attribute");

        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
    }

    @Test
    public void sessionsToSameKeyspaceShareStatistics() {
        SessionImpl session2 = server.session(session.keyspace().name());
        assertSame(session.keyspaceStatistics(), session2.keyspaceStatistics());
    }


    // ------- persistence is TODO
    // only work on tests that can be performed in-memory

}

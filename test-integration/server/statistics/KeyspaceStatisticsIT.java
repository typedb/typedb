package grakn.core.server.statistics;

import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionFactory;
import grakn.core.server.session.SessionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KeyspaceStatisticsIT {

    private SessionImpl session;
    private SessionFactory sessionFactory;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        sessionFactory = server.sessionFactory();
    }

    @After
    public void closeSession() { session.close(); }


    @Test
    public void newKeyspaceHasZeroBaseCounts() {
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
        assertTrue(session.keyspaceStatistics() == session2.keyspaceStatistics());
    }


    // ------- persistence is TODO
    // only work on tests that can be performed in-memory

}

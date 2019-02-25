package grakn.core.server.session;

import grakn.core.rule.GraknTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SessionFactoryIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();
    private SessionImpl session;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void whenDeletingKeyspace_OpenSessionsFail() {

    }

    @Test
    public void whenDeletingSameKeyspaceTwice_NoErrorThrown() {

    }

    @Test
    public void whenKeyspaceDeleted_OpeningSessionRecreatesKeyspace() {

    }
}

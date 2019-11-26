package grakn.core.graph;

import grakn.core.graph.diskstorage.cql.CQLStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.kb.server.Session;
import grakn.core.rule.GraknTestServer;
import org.junit.Before;
import org.junit.ClassRule;

public class MultiWriteKeyColumnValueStoreIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private final KeyColumnValueStoreManager storeManager;
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        storeManager = new CQLStoreManager(getConfig());
    }

}

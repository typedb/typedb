package ai.grakn.test.engine;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.test.EngineContext;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.engine.SystemKeyspace.SYSTEM_KB_KEYSPACE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest {

    private final Function<String, GraknTx> engineFactoryGraphProvider = (k) -> engine.server().factory().tx(k, GraknTxType.WRITE);
    private final Function<String, GraknTx> externalFactoryGraphProvider = (k) -> Grakn.session(engine.uri(), k).open(GraknTxType.WRITE);

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final Set<GraknTx> transactions = new HashSet<>();

    @After
    public void cleanSystemKeySpaceGraph(){
        try (GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            graph.getEntityType("keyspace").instances().forEach(Concept::delete);
            graph.commit();
        }

        transactions.forEach(GraknTx::close);
    }

    @Test
    public void whenCreatingGraphsUsingEngineFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(engineFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenClearingGraphsUsingExternalFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(externalFactoryGraphProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a graph entirely
        GraknTx deletedGraph = txs.iterator().next();
        deletedGraph.admin().delete();
        txs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx graph:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    @Test
    public void whenClearingGraphsUsingEngineFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(engineFactoryGraphProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a graph entirely
        GraknTx deletedGraph = txs.iterator().next();
        deletedGraph.admin().delete();
        txs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx graph:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    private void setVersionInSystemGraph(String version){
        String versionResourceType = "system-version";

        //Insert fake version number
        try(GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            //Delete old version number
            graph.getAttributeType(versionResourceType).instances().forEach(Concept::delete);
            //Add Fake Version
            graph.getAttributeType(versionResourceType).putAttribute(version);
            graph.commit();
        }
    }

    private Set<GraknTx> buildTxs(Function<String, GraknTx> txProvider, String ... keyspaces){
        Set<GraknTx> newTransactions = Arrays.stream(keyspaces)
                .map(txProvider)
                .collect(Collectors.toSet());
        transactions.addAll(newTransactions);
        return newTransactions;
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)){
            AttributeType<String> keyspaceName = graph.getAttributeType("keyspace-name");
            return graph.getEntityType("keyspace").instances().
                    map(e -> e.attributes(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}

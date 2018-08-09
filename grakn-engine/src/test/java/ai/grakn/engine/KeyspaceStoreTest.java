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

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.engine.attribute.uniqueness.AttributeUniqueness;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.KeyspaceService;
import ai.grakn.engine.rpc.OpenRequest;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.rpc.SessionService;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.SimpleURI;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.engine.KeyspaceStore.SYSTEM_KB_KEYSPACE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KeyspaceStoreTest {

    private static final int PORT = 6666;

    private static final GraknConfig config = GraknConfig.create();
    private static final LockProvider lockProvider = mock(LockProvider.class);
    private static EngineGraknTxFactory graknFactory;
    private static KeyspaceStore keyspaceStore;
    private static Grakn graknClient;


    //Needed to start cass depending on profile
    @ClassRule
    public static final SessionContext sessionContext = SessionContext.create();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final Function<String, GraknTx> engineFactoryKBProvider = (k) -> graknFactory.tx(Keyspace.of(k), GraknTxType.WRITE);
    private final Function<String, GraknTx> externalFactoryGraphProvider = (k) -> graknClient.session(Keyspace.of(k)).transaction(GraknTxType.WRITE);

    private final Set<GraknTx> transactions = new HashSet<>();

    private static ServerRPC rpcServerRPC;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final static AttributeUniqueness mockedAttributeUniqueness = mock(AttributeUniqueness.class);


    @BeforeClass
    public static void setup() throws IOException {
        keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();
        graknFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);
        OpenRequest requestOpener = new ServerOpenRequest(graknFactory);
        io.grpc.Server server = ServerBuilder.forPort(PORT)
                .addService(new SessionService(requestOpener, mockedAttributeUniqueness))
                .addService(new KeyspaceService(keyspaceStore))
                .build();
        rpcServerRPC = ServerRPC.create(server);
        rpcServerRPC.start();

        graknClient = new Grakn(new SimpleURI(config.getProperty(GraknConfigKey.SERVER_HOST_NAME)+":"+PORT));

        Lock lock = mock(Lock.class);
        when(lockProvider.getLock(any())).thenReturn(lock);
    }

    @After
    public void cleanSystemKeySpaceGraph(){
        try (GraknTx tx = graknFactory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            tx.getEntityType("keyspace").instances().forEach(Concept::delete);
            tx.getAttributeType("keyspace-name").instances().forEach(Concept::delete);
            tx.commit();
        }

        transactions.forEach(GraknTx::close);
    }

    @Test
    public void whenCreatingGraphsUsingEngineFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(engineFactoryKBProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system keyspace", spaces.contains(keyspace));
            assertTrue(keyspaceStore.containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s4", "s5", "s6"};

        buildTxs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system keyspace", spaces.contains(keyspace));
            assertTrue(keyspaceStore.containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenClearingGraphsUsingExternalFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(externalFactoryGraphProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a tx entirely
        GraknTx deletedTx = txs.iterator().next();
        keyspaceStore.deleteKeyspace(deletedTx.keyspace());
        txs.remove(deletedTx);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx tx:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(tx.keyspace().getValue()));
        }
        assertFalse(keyspaceStore.containsKeyspace(deletedTx.keyspace()));
    }

    @Test
    public void whenClearingGraphsUsingEngineFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g4", "g5", "g6"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(engineFactoryKBProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a keyspace entirely
        GraknTx deletedTx = txs.iterator().next();
        keyspaceStore.deleteKeyspace(deletedTx.keyspace());
        txs.remove(deletedTx);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx tx:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(tx.keyspace().getValue()));
        }
        assertFalse(keyspaceStore.containsKeyspace(deletedTx.keyspace()));
    }
    private Set<GraknTx> buildTxs(Function<String, GraknTx> txProvider, String ... keyspaces){
        Set<GraknTx> newTransactions = Arrays.stream(keyspaces)
                .map(txProvider)
                .collect(Collectors.toSet());
        transactions.addAll(newTransactions);
        return newTransactions;
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknTx tx = graknFactory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)){
            AttributeType<String> keyspaceName = tx.getAttributeType("keyspace-name");
            return tx.getEntityType("keyspace").instances().
                    map(e -> e.attributes(keyspaceName).iterator().next().value().toString()).
                    collect(Collectors.toSet());
        }
    }
}

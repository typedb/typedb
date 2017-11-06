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

package ai.grakn.engine;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
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
        try (GraknTx tx = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            tx.getEntityType("keyspace").instances().forEach(Concept::delete);
            tx.getAttributeType("keyspace-name").instances().forEach(Concept::delete);
            tx.commit();
        }

        transactions.forEach(GraknTx::close);
    }

    @Test
    public void whenCreatingGraphsUsingEngineFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(engineFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system keyspace", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system keyspace", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenClearingGraphsUsingExternalFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(externalFactoryGraphProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a tx entirely
        GraknTx deletedGraph = txs.iterator().next();
        deletedGraph.admin().delete();
        txs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx tx:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(tx.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    @Test
    public void whenClearingGraphsUsingEngineFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create transactions to begin with
        Set<GraknTx> txs = buildTxs(engineFactoryGraphProvider, keyspaces);
        txs.forEach(GraknTx::close);

        //Delete a tx entirely
        GraknTx deletedGraph = txs.iterator().next();
        deletedGraph.admin().delete();
        txs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx tx:txs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(tx.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }
    private Set<GraknTx> buildTxs(Function<String, GraknTx> txProvider, String ... keyspaces){
        Set<GraknTx> newTransactions = Arrays.stream(keyspaces)
                .map(txProvider)
                .collect(Collectors.toSet());
        transactions.addAll(newTransactions);
        return newTransactions;
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknTx tx = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)){
            AttributeType<String> keyspaceName = tx.getAttributeType("keyspace-name");
            return tx.getEntityType("keyspace").instances().
                    map(e -> e.attributes(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}

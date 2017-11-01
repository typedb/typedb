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
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.util.MockRedisRule;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.BeforeClass;
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
    private final Function<String, GraknTx> engineFactoryGraphProvider = (k) -> EngineTestHelper.factory().tx(k, GraknTxType.WRITE);
    private final Function<String, GraknTx> externalFactoryGraphProvider = (k) -> Grakn.session(EngineTestHelper.uri(), k).open(GraknTxType.WRITE);

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(EngineTestHelper.config().getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final Set<GraknTx> transactions = new HashSet<>();

    @BeforeClass
    public static void beforeClass() {
        EngineTestHelper.engineWithKBs();
    }

    @After
    public void cleanSystemKeySpaceGraph(){
        try (GraknTx graph = EngineTestHelper.factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
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
            assertTrue(EngineTestHelper.factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        buildTxs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(EngineTestHelper.factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
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
        assertFalse(EngineTestHelper.factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
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
        assertFalse(EngineTestHelper.factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    private Set<GraknTx> buildTxs(Function<String, GraknTx> txProvider, String ... keyspaces){
        Set<GraknTx> newTransactions = Arrays.stream(keyspaces)
                .map(txProvider)
                .collect(Collectors.toSet());
        transactions.addAll(newTransactions);
        return newTransactions;
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknTx graph = EngineTestHelper.factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)){
            AttributeType<String> keyspaceName = graph.getAttributeType("keyspace-name");
            return graph.getEntityType("keyspace").instances().
                    map(e -> e.attributes(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}

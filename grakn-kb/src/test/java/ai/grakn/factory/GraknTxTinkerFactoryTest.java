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

package ai.grakn.factory;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.GraknTxTinker;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Paths;

import static ai.grakn.util.ErrorMessage.TRANSACTION_ALREADY_OPEN;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraknTxTinkerFactoryTest {
    private final static File TEST_CONFIG_FILE = Paths.get("../conf/test/tinker/grakn.properties").toFile();
    private final static GraknConfig TEST_CONFIG = GraknConfig.read(TEST_CONFIG_FILE);
    private final static EmbeddedGraknSession session = mock(EmbeddedGraknSession.class);
    private TxFactory tinkerGraphFactory;


    @BeforeClass
    public static void setup(){
        when(session.config()).thenReturn(TEST_CONFIG);
    }

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupTinkerGraphFactory(){
        tinkerGraphFactory = new TxFactoryTinker(session);
    }

    @Test
    public void whenBuildingGraphUsingTinkerFactory_ReturnGraknTinkerGraph() throws Exception {
        GraknTx graph = tinkerGraphFactory.open(GraknTxType.WRITE);
        assertThat(graph, instanceOf(GraknTxTinker.class));
        assertThat(graph, instanceOf(EmbeddedGraknTx.class));
    }

    @Test
    public void whenBuildingTxFromTheSameFactory_ReturnSingletonGraphs(){
        GraknTx tx1 = tinkerGraphFactory.open(GraknTxType.WRITE);
        TinkerGraph tinkerGraph1 = ((GraknTxTinker) tx1).getTinkerPopGraph();
        tx1.close();
        GraknTx tx1_copy = tinkerGraphFactory.open(GraknTxType.WRITE);
        tx1_copy.close();

        GraknTx tx2 = tinkerGraphFactory.open(GraknTxType.BATCH);
        TinkerGraph tinkerGraph2 = ((GraknTxTinker) tx2).getTinkerPopGraph();
        tx2.close();
        GraknTx tx2_copy = tinkerGraphFactory.open(GraknTxType.BATCH);

        assertEquals(tx1, tx1_copy);
        assertEquals(tx2, tx2_copy);

        assertNotEquals(tx1, tx2);
        assertEquals(tinkerGraph1, tinkerGraph2);
    }

    @Test
    public void whenRetrievingGraphFromGraknTinkerGraph_ReturnTinkerGraph(){
        assertThat(tinkerGraphFactory.getTinkerPopGraph(false), instanceOf(TinkerGraph.class));
    }

    @Test
    public void whenGettingGraphFromFactoryWithAlreadyOpenGraph_Throw(){
        Keyspace mytest = Keyspace.of("mytest");
        when(session.keyspace()).thenReturn(mytest);
        TxFactoryTinker factory = new TxFactoryTinker(session);
        factory.open(GraknTxType.WRITE);
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(TRANSACTION_ALREADY_OPEN.getMessage("mytest"));
        factory.open(GraknTxType.WRITE);
    }

    @Test
    public void whenGettingGraphFromFactoryClosingItAndGettingItAgain_ReturnGraph(){
        TxFactoryTinker factory = new TxFactoryTinker(session);
        GraknTx graph1 = factory.open(GraknTxType.WRITE);
        graph1.close();
        GraknTxTinker graph2 = factory.open(GraknTxType.WRITE);
        assertEquals(graph1, graph2);
    }

}
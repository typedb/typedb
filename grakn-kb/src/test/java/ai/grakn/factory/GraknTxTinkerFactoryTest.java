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

package ai.grakn.factory;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.GraknTxTinker;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static ai.grakn.util.ErrorMessage.TRANSACTION_ALREADY_OPEN;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraknTxTinkerFactoryTest {
    private final static String TEST_CONFIG = "../conf/test/tinker/grakn.properties";
    private final static Properties TEST_PROPERTIES = new Properties();
    private final static GraknSession session = mock(GraknSession.class);
    private TxFactory tinkerGraphFactory;


    @BeforeClass
    public static void setup(){
        try (InputStream in = new FileInputStream(TEST_CONFIG)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw GraknTxOperationException.invalidConfig(TEST_CONFIG);
        }
        when(session.config()).thenReturn(TEST_PROPERTIES);
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
        assertThat(graph, instanceOf(GraknTxAbstract.class));
    }

    @Test
    public void whenBuildingGraphFromTheSameFactory_ReturnSingletonGraphs(){
        GraknTx graph1 = tinkerGraphFactory.open(GraknTxType.WRITE);
        TinkerGraph tinkerGraph1 = ((GraknTxTinker) graph1).getTinkerPopGraph();
        graph1.close();
        GraknTx graph1_copy = tinkerGraphFactory.open(GraknTxType.WRITE);
        graph1_copy.close();

        GraknTx graph2 = tinkerGraphFactory.open(GraknTxType.BATCH);
        TinkerGraph tinkerGraph2 = ((GraknTxTinker) graph2).getTinkerPopGraph();
        graph2.close();
        GraknTx graph2_copy = tinkerGraphFactory.open(GraknTxType.BATCH);

        assertEquals(graph1, graph1_copy);
        assertEquals(graph2, graph2_copy);

        assertNotEquals(graph1, graph2);
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
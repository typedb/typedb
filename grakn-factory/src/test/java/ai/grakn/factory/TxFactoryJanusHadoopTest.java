/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TxFactoryJanusHadoopTest {
    private final static EmbeddedGraknSession session = mock(EmbeddedGraknSession.class);
    private static final File TEST_CONFIG_FILE = Paths.get("../conf/main/grakn.properties").toFile();
    private final static GraknConfig TEST_CONFIG = GraknConfig.read(TEST_CONFIG_FILE);

    private TxFactoryJanusHadoop factory;

    @Before
    public void setUp() {
        when(session.keyspace()).thenReturn(Keyspace.of("rubbish"));
        when(session.uri()).thenReturn("rubbish");
        when(session.config()).thenReturn(TxFactoryJanusHadoopTest.TEST_CONFIG);
        factory = new TxFactoryJanusHadoop(session);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void buildGraknGraphFromTinker() {
        factory.open(GraknTxType.WRITE);
    }

    @Test
    public void buildTinkerPopGraph() {
        assertThat(factory.getTinkerPopGraph(false), instanceOf(HadoopGraph.class));
    }

    @Test
    public void testSingleton(){
        HadoopGraph graph1 = factory.getTinkerPopGraph(false);
        HadoopGraph graph2 = factory.getTinkerPopGraph(false);

        assertEquals(graph1, graph2);
    }

}
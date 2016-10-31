/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.factory;

import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraknTitanHadoopGraphFactoryTest {
    private final String TEST_CONFIG = "../conf/main/grakn-analytics.properties";

    private GraknTitanHadoopInternalFactory factory;

    @Before
    public void setUp() throws Exception {
        factory = new GraknTitanHadoopInternalFactory("rubbish", "rubbish", TEST_CONFIG);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void buildMindmapsGraphFromTinker() throws Exception {
        factory.getGraph(false);
    }

    @Test
    public void buildTinkerPopGraph() throws Exception {
        assertThat(factory.getTinkerPopGraph(false), instanceOf(HadoopGraph.class));
    }

    @Test
    public void testSingleton(){
        HadoopGraph graph1 = factory.getTinkerPopGraph(false);
        HadoopGraph graph2 = factory.getTinkerPopGraph(false);

        assertEquals(graph1, graph2);
    }

}
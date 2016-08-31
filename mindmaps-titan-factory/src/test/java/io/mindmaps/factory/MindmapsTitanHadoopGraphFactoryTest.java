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

package io.mindmaps.factory;

import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class MindmapsTitanHadoopGraphFactoryTest {
    private final String TEST_CONFIG = "../conf/main/mindmaps-analytics.properties";

    private MindmapsTitanHadoopGraphFactory factory;

    @Before
    public void setUp() throws Exception {
        factory = new MindmapsTitanHadoopGraphFactory();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void buildMindmapsGraphFromTinker() throws Exception {
        factory.getGraph("rubbish", "rubbish", TEST_CONFIG, false);
    }

    @Test
    public void buildTinkerPopGraph() throws Exception {
        assertThat(factory.getTinkerPopGraph("rubbish", "Rubbish", TEST_CONFIG, false), instanceOf(HadoopGraph.class));
    }

}
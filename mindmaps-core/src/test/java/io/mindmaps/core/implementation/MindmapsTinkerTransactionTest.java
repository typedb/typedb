/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class MindmapsTinkerTransactionTest {
    MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testRefresh() throws Exception {
        mindmapsGraph.newTransaction().refresh();
    }

    @Test
    public void testGetRootGraph() throws Exception {
        MindmapsTinkerTransaction tinkerTransaction = (MindmapsTinkerTransaction) mindmapsGraph.newTransaction();
        assertThat(tinkerTransaction.getRootGraph(), instanceOf(MindmapsTinkerGraph.class));
    }
}
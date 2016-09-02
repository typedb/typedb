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

import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotEquals;

public class MindmapsTestGraphFactoryTest {

    @Test(expected=InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<MindmapsTestGraphFactory> c = MindmapsTestGraphFactory.class.getDeclaredConstructor();
        c.setAccessible(true);
        c.newInstance();
    }

    @Test
    public void testNewEmptyGraph(){
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        assertNotNull(graph);
        assertFalse(graph.isBatchLoadingEnabled());
    }

    @Test
    public void testDifferentEmptyGraphs(){
        AbstractMindmapsGraph graph1 = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        AbstractMindmapsGraph graph2 = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();

        assertNotEquals(graph1, graph2);
        assertNotEquals(graph1.getGraph(), graph2.getGraph());
        assertNotEquals(graph1.getTransaction(), graph2.getTransaction());
    }

    @Test
    public void testNewEmptyGraphBatchLoading(){
        MindmapsGraph graph = MindmapsTestGraphFactory.newBatchLoadingEmptyGraph();
        assertNotNull(graph);
        assertTrue(graph.isBatchLoadingEnabled());
    }
}
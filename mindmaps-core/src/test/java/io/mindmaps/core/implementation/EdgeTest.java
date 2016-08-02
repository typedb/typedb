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

import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EdgeTest {

    private MindmapsTransactionImpl mindmapsTransaction;
    private EntityType entityType;
    private Entity entity;
    private EdgeImpl edge;

    @Before
    public void setUp(){
        mindmapsTransaction = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        entityType = mindmapsTransaction.putEntityType("My Entity Type");
        entity = mindmapsTransaction.putEntity("My entity", entityType);
        Edge tinkerEdge = mindmapsTransaction.getTinkerTraversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), entity.getId()).outE().next();
        edge = new EdgeImpl(tinkerEdge, mindmapsTransaction);
    }

    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsTransaction.close();
    }

    @Test
    public void testGetSource() throws Exception {
        assertEquals(entity, edge.getSource());
    }

    @Test
    public void testGetTarget() throws Exception {
        assertEquals(entityType, edge.getTarget());
    }

    @Test
    public void testGetType() throws Exception {
        assertEquals(DataType.EdgeLabel.ISA, edge.getType());
    }

    @Test
    public void testProperty() throws Exception {
        edge.setProperty(DataType.EdgeProperty.ROLE_TYPE, "role");
        assertEquals("role", edge.getProperty(DataType.EdgeProperty.ROLE_TYPE));
    }

}
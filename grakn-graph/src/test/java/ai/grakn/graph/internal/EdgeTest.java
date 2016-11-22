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

package ai.grakn.graph.internal;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class EdgeTest extends GraphTestBase{

    private EntityType entityType;
    private Entity entity;
    private EdgeImpl edge;

    @Before
    public void setUp(){
        entityType = graknGraph.putEntityType("My Entity Type");
        entity = entityType.addEntity();
        Edge tinkerEdge = (Edge) graknGraph.getTinkerTraversal().hasId(entity.getId()).outE().next();
        edge = new EdgeImpl(tinkerEdge, graknGraph);
    }

    @Test
    public void testEquals(){
        Entity entity2 = entityType.addEntity();
        Edge tinkerEdge = (Edge) graknGraph.getTinkerTraversal().hasId(entity2.getId()).outE().next();
        EdgeImpl edge2 = new EdgeImpl(tinkerEdge, graknGraph);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
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
        assertEquals(Schema.EdgeLabel.ISA, edge.getType());
    }

    @Test
    public void testProperty() throws Exception {
        edge.setProperty(Schema.EdgeProperty.ROLE_TYPE, "role");
        assertEquals("role", edge.getProperty(Schema.EdgeProperty.ROLE_TYPE));
        assertNull(edge.getProperty(Schema.EdgeProperty.FROM_TYPE_NAME));
    }

}
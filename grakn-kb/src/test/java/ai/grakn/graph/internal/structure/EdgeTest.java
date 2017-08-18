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

package ai.grakn.graph.internal.structure;

import ai.grakn.concept.Entity;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.concept.EntityImpl;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.structure.EdgeElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EdgeTest extends GraphTestBase {

    private EntityTypeImpl entityType;
    private EntityImpl entity;
    private EdgeElement edge;

    @Before
    public void createEdge(){
        entityType = (EntityTypeImpl) graknGraph.putEntityType("My Entity Type");
        entity = (EntityImpl) entityType.addEntity();
        Edge tinkerEdge = graknGraph.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), entity.getId().getValue()).outE().next();
        edge = new EdgeElement(graknGraph, tinkerEdge);
    }

    @Test
    public void checkEqualityBetweenEdgesBasedOnID(){
        Entity entity2 = entityType.addEntity();
        Edge tinkerEdge = graknGraph.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), entity2.getId().getValue()).outE().next();
        EdgeElement edge2 = new EdgeElement(graknGraph, tinkerEdge);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
    }

    @Test
    public void whenGettingTheSourceOfAnEdge_ReturnTheConceptTheEdgeComesFrom() throws Exception {
        assertEquals(entity.vertex(), edge.source());
    }

    @Test
    public void whenGettingTheTargetOfAnEdge_ReturnTheConceptTheEdgePointsTowards() throws Exception {
        assertEquals(entityType.currentShard().vertex(), edge.target());
    }

    @Test
    public void whenGettingTheLabelOfAnEdge_ReturnExpectedType() throws Exception {
        assertEquals(Schema.EdgeLabel.ISA.getLabel(), edge.label());
    }
}
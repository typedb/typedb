/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.concept.structure;


import grakn.core.common.config.Config;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.core.server.session.SessionImpl;
import grakn.core.util.ConceptDowncasting;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EdgeIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private SessionImpl session;
    private Transaction tx;
    private JanusTraversalSourceProvider janusTraversalSourceProvider;
    private EntityType entityType;
    private Entity entity;
    private EdgeElement edge;

    @Before
    public void setUp(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        tx = session.writeTransaction();

        // Create Edge
        entityType = tx.putEntityType("My Entity Type");
        entity = entityType.create();
        janusTraversalSourceProvider = ((TestTransactionProvider.TestTransaction)tx).janusTraversalSourceProvider();
        ElementFactory elementFactory = ((TestTransactionProvider.TestTransaction)tx).elementFactory();

        Edge tinkerEdge = janusTraversalSourceProvider.getTinkerTraversal().V().hasId(Schema.elementId(entity.id())).outE().next();
        edge = new EdgeElementImpl(elementFactory, tinkerEdge);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void checkEqualityBetweenEdgesBasedOnID() {
        Entity entity2 = entityType.create();
        Edge tinkerEdge = janusTraversalSourceProvider.getTinkerTraversal().V().hasId(Schema.elementId(entity2.id())).outE().next();
        EdgeElement edge2 = new EdgeElementImpl(null, tinkerEdge);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
    }

    @Test
    public void whenGettingTheSourceOfAnEdge_ReturnTheConceptTheEdgeComesFrom() {
        assertEquals(ConceptDowncasting.concept(entity).vertex(), edge.source());
    }

    @Test
    public void whenGettingTheTargetOfAnEdge_ReturnTheConceptTheEdgePointsTowards() {
        assertEquals(ConceptDowncasting.type(entityType).currentShard().vertex(), edge.target());
    }

    @Test
    public void whenGettingTheLabelOfAnEdge_ReturnExpectedType() {
        assertEquals(Schema.EdgeLabel.ISA.getLabel(), edge.label());
    }
}

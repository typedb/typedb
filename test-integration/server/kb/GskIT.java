/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.kb;

import com.google.common.collect.Sets;
import grakn.client.GraknClient;
import grakn.core.concept.ConceptId;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static graql.lang.Graql.define;
import static graql.lang.Graql.insert;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

// TODO: remove
public class GskIT {
    @ClassRule
    public static GraknTestServer graknTestServer = new GraknTestServer();
    private JanusGraphFactory janusGraphFactory = new JanusGraphFactory(graknTestServer.serverConfig());
    private SessionImpl session;
    private JanusGraph janusGraph;
    private static final Logger LOG = LoggerFactory.getLogger(GskIT.class);

    @Before
    public void before() {
        session = graknTestServer.sessionWithNewKeyspace();
        janusGraph = janusGraphFactory.openGraph(session.keyspace().name());
    }

    @After
    public void after() {
        session.close();
        janusGraph.close();
    }

    @Test
    public void verifyThatTypeShardingIsPerformedOnAGivenTypeIfThresholdIsReached() {
        TransactionOLTP.TYPE_SHARD_THRESHOLD = 1;
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(define(type("person").sub("entity")).asDefine());
            tx.commit();
        }
        ConceptId p1;
        try (TransactionOLTP tx = session.transaction().write()) {
            p1 = tx.execute(insert(var("p1").isa("person")).asInsert()).get(0).get("p1").id();
            tx.commit();
        }
        Set<Vertex> typeShards = janusGraph.traversal().V().hasLabel(Schema.VertexProperty.SCHEMA_LABEL.name(), "SHARD").toSet();
        assertEquals(1, typeShards.size());
        Vertex typeShardForP1 = janusGraph.traversal().V(p1.getValue().substring(1)).out(Schema.EdgeLabel.ISA.getLabel()).toList().get(0);
        assertEquals(typeShards.iterator().next(), typeShardForP1);
        ConceptId p2;
        try (TransactionOLTP tx = session.transaction().write()) {
            p2 = tx.execute(insert(var("p2").isa("person")).asInsert()).get(0).get("p2").id();
            tx.commit();
        }
        // TODO: verify that a new type shard is created
        typeShards = janusGraph.traversal().V().hasLabel(Schema.VertexProperty.SCHEMA_LABEL.name(), "SHARD").toSet();
        assertEquals(2, typeShards.size());
        Vertex typeShardForP2 = janusGraph.traversal().V(p2.getValue().substring(1)).out(Schema.EdgeLabel.ISA.getLabel()).toSet().iterator().next();
        assertEquals(Sets.difference(typeShards, Sets.newHashSet(typeShardForP1)).iterator().next(), typeShardForP2);
    }

    @Test
    public void verifyThatTypeShardingIsPerformedOnTheRightEntity() {
        TransactionOLTP.TYPE_SHARD_THRESHOLD = 1;
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(define(type("person").sub("entity")).asDefine());
            tx.execute(define(type("company").sub("entity")).asDefine());
            tx.commit();
        }

        // insert two people and a company
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(insert(var("p").isa("person")).asInsert());
            tx.execute(insert(var("p").isa("person")).asInsert());
            tx.execute(insert(var("c").isa("company")).asInsert());
            tx.commit();
        }

        // TODO: verify that 'person' is sharded
        //  - verify that a new type shard is created
        //  - and is created for the type 'person'
        // TODO: verify that 'company' is not sharded
    }

    static class TypeShardTest {
        @Test
        public void verifyThatShardAlgorithmWorks() {
            // shard returns void. how should I test it? how should I change the interface in order to make it testable?
        }
    }
}

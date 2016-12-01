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

package ai.grakn.test.graql.analytics;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.BulkResourceMutate;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.graql.internal.query.analytics.AbstractComputeQuery;
import ai.grakn.test.AbstractGraphTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class DegreeTest extends AbstractGraphTest {

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(AbstractComputeQuery.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(BulkResourceMutate.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        String entity1 = thing.addEntity().getId();
        String entity2 = thing.addEntity().getId();
        String entity3 = thing.addEntity().getId();
        String entity4 = anotherThing.addEntity().getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity1))
                .putRolePlayer(role2, graph.getConcept(entity2))
                .getId();
        String id2 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity3))
                .getId();
        String id3 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity4))
                .getId();
        graph.commit();

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(entity4, 1L);
        correctDegrees.put(id1, 2L);
        correctDegrees.put(id2, 2L);
        correctDegrees.put(id3, 2L);

        // compute degrees
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));

        // compute degrees again after persisting degrees
        graph.graql().compute().degree().persist().execute();
        Map<Long, Set<String>> degrees2 = graph.graql().compute().degree().execute();
        assertEquals(degrees.size(), degrees2.size());
        degrees2.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));

        // compute degrees on subgraph
        graph = factory.getGraph();
        Map<Long, Set<String>> degrees3 = graph.graql().compute().degree().in("thing", "related").execute();
        correctDegrees.put(id3, 1L);
        assertTrue(!degrees3.isEmpty());
        degrees3.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));
    }

    private void checkDegrees(Map<String, Long> correctDegrees) {
        correctDegrees.entrySet().forEach(entry -> {
            Collection<Resource<?>> resources =
                    graph.<Instance>getConcept(entry.getKey()).resources(graph.getResourceType(Analytics.degree));
            assertEquals(1, resources.size());
            assertEquals(entry.getValue(), resources.iterator().next().getValue());
        });
    }
}
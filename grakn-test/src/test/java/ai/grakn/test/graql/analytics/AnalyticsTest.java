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

import ai.grakn.Grakn;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.GraphContext;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class AnalyticsTest {

    @Rule
    public final GraphContext context = GraphContext.empty();

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(ComputeQuery.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testInferredResourceRelation() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        String resourceTypeName = "degree";
        ResourceType<Long> degree = context.graph().putResourceType(resourceTypeName, ResourceType.DataType.LONG);
        EntityType thing = context.graph().putEntityType("thing");
        thing.hasResource(degree);

        Entity thisThing = thing.addEntity();
        Resource thisResource = degree.putResource(1L);
        thisThing.hasResource(thisResource);
        context.graph().commit();

        Map<Long, Set<String>> degrees;
        degrees = context.graph().graql().compute().degree().of("thing").in("thing", "degree").execute();
        assertEquals(1, degrees.size());
        assertEquals(1, degrees.get(1L).size());

        degrees = context.graph().graql().compute().degree().in("thing", "degree").execute();
        assertEquals(1, degrees.size());
        assertEquals(2, degrees.get(1L).size());
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make slightly odd context.graph()
        String resourceTypeId = "degree";
        EntityType thing = context.graph().putEntityType("thing");

        context.graph().putResourceType(resourceTypeId, ResourceType.DataType.LONG);
        RoleType degreeOwner = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeId));
        RoleType degreeValue = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeId));
        RelationType relationType = context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);
        thing.playsRole(degreeOwner);

        Entity thisThing = thing.addEntity();
        relationType.addRelation().putRolePlayer(degreeOwner, thisThing);
        context.graph().commit();

        // the null role-player caused analytics to fail at some stage
        try {
            context.graph().graql().compute().degree().execute();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Ignore //TODO: Fix remotely. Failing on Jenkins only
    @Test
    public void testResourcesMergedOnBulkMutate() throws GraknValidationException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());
        Cache cache = Cache.getInstance();

        //Clear Cache
        cache.getKeyspaces().forEach(keyspace -> {
            cache.getResourceJobs(keyspace).clear();
            cache.getCastingJobs(keyspace).clear();
        });

        RoleType friend1 = context.graph().putRoleType("friend1");
        RoleType friend2 = context.graph().putRoleType("friend2");
        RelationType friendship = context.graph().putRelationType("friendship");
        friendship.hasRole(friend1).hasRole(friend2);

        EntityType person = context.graph().putEntityType("person");
        person.playsRole(friend1).playsRole(friend2);

        for (int i = 0; i < 10; i++) {
            friendship.addRelation()
                    .putRolePlayer(friend1, person.addEntity())
                    .putRolePlayer(friend2, person.addEntity());
        }

        context.graph().commit();
        String keyspace = context.graph().getKeyspace();

        //TODO: Replace the following persistence behaviour
//        graph.graql().compute().degree().persist().execute();

        Collection<Resource<Object>> degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType(Schema.Analytics.DEGREE.getName()).instances();
        assertTrue(degrees.size() > 1);

        //Wait for cache to be updated
        int failCount = 0;
        while (cache.getResourceJobs(keyspace).size() < 4) {
            Thread.sleep(1000);
            failCount++;
            assertFalse("Failed to update cache with resources to merge", failCount < 10);
        }

        //Force Post Processing
        PostProcessing postProcessing = PostProcessing.getInstance();
        postProcessing.run();

        //Check all is good
        context.graph().close();
        degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType("degree").instances();
        assertEquals(2, degrees.size());
    }
}
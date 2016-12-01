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
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.graql.internal.query.analytics.AbstractComputeQuery;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class AnalyticsTest extends AbstractGraphTest {

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(AbstractComputeQuery.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make slightly odd graph
        String resourceTypeId = "degree";
        EntityType thing = graph.putEntityType("thing");

        graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG);
        RoleType degreeOwner = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeId));
        RelationType relationType = graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);
        thing.playsRole(degreeOwner);

        Entity thisThing = thing.addEntity();
        relationType.addRelation().putRolePlayer(degreeOwner, thisThing);
        graph.commit();

        // the null role-player caused analytics to fail at some stage
        try {
            graph.graql().compute().degree().persist().execute();
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

        RoleType friend1 = graph.putRoleType("friend1");
        RoleType friend2 = graph.putRoleType("friend2");
        RelationType friendship = graph.putRelationType("friendship");
        friendship.hasRole(friend1).hasRole(friend2);

        EntityType person = graph.putEntityType("person");
        person.playsRole(friend1).playsRole(friend2);

        for (int i = 0; i < 10; i++) {
            friendship.addRelation()
                    .putRolePlayer(friend1, person.addEntity())
                    .putRolePlayer(friend2, person.addEntity());
        }

        graph.commit();
        String keyspace = graph.getKeyspace();

        graph.graql().compute().degree().persist().execute();

        Collection<Resource<Object>> degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType(AbstractComputeQuery.degree).instances();
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
        graph.close();
        degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType("degree").instances();
        assertEquals(2, degrees.size());
    }
}
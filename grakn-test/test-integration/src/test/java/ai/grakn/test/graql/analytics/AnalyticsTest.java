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

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class AnalyticsTest {

    @ClassRule
    public static final EngineContext context = EngineContext.startInMemoryServer();
    private GraknSession factory;

    private static final String thingy = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private String relationId12;
    private String relationId24;

    @Before
    public void setUp() {
        factory = context.factoryWithNewKeyspace();
    }

    @Ignore // No longer applicable
    @Test
    public void testInferredResourceRelation() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            Label resourceLabel = Label.of("degree");
            ResourceType<Long> degree = graph.putResourceType(resourceLabel, ResourceType.DataType.LONG);
            EntityType thingy = graph.putEntityType("thingy");
            thingy.resource(degree);

            Entity thisThing = thingy.addEntity();
            Resource thisResource = degree.putResource(1L);
            thisThing.resource(thisResource);
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Map<Long, Set<String>> degrees;
            degrees = graph.graql().compute().degree().of("thingy").in("thingy", "degree").execute();
            assertEquals(1, degrees.size());
            assertEquals(1, degrees.get(1L).size());

            degrees = graph.graql().compute().degree().in("thingy", "degree").execute();
            assertEquals(1, degrees.size());
            assertEquals(2, degrees.get(1L).size());
        }
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            // make slightly odd graph
            Label resourceTypeId = Label.of("degree");
            EntityType thingy = graph.putEntityType("thingy");

            graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG);
            Role degreeOwner = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeId));
            Role degreeValue = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeId));
            RelationType relationType = graph.putRelationType(Schema.ImplicitType.HAS.getLabel(resourceTypeId))
                    .relates(degreeOwner)
                    .relates(degreeValue);
            thingy.plays(degreeOwner);

            Entity thisThing = thingy.addEntity();
            relationType.addRelation().addRolePlayer(degreeOwner, thisThing);

            graph.commit();
        }

        // the null role-player caused analytics to fail at some stage
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            graph.graql().compute().degree().execute();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testConcurrentAnalyticsJobsBySubmittingGraqlComputeQueries() {
        // TODO: move parallel tests to integration tests
        assumeFalse(GraknTestSetup.usingTinker());

        addOntologyAndEntities();

        List<String> queryList = new ArrayList<>();
        queryList.add("compute count;");
        queryList.add("compute cluster;");
        queryList.add("compute degrees;");
        queryList.add("compute path from \"" + entityId1 + "\" to \"" + entityId4 + "\";");

        Set<?> result = queryList.parallelStream().map(query -> {
            try (GraknGraph graph = factory.open(GraknTxType.READ)) {
                return graph.graql().parse(query).execute();
            }
        }).collect(Collectors.toSet());
        assertEquals(queryList.size(), result.size());
    }

    private void addOntologyAndEntities() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thingy);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Entity entity1 = entityType1.addEntity();
            Entity entity2 = entityType1.addEntity();
            Entity entity3 = entityType1.addEntity();
            Entity entity4 = entityType2.addEntity();

            entityId1 = entity1.getId().getValue();
            entityId2 = entity2.getId().getValue();
            entityId3 = entity3.getId().getValue();
            entityId4 = entity4.getId().getValue();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationType relationType = graph.putRelationType(related).relates(role1).relates(role2);

            relationId12 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId().getValue();
            relationId24 = relationType.addRelation()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId().getValue();

            graph.commit();
        }
    }
}
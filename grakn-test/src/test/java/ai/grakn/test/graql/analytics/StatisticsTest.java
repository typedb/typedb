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
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Sets;
import ai.grakn.exception.GraknValidationException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class StatisticsTest extends AbstractGraphTest {

    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;

    String keyspace;
    Analytics computer;
    double delta = 0.000001;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testStatisticsExceptions() throws Exception {
        addOntologyAndEntities();
        addResourceRelations();

        //TODO: add more detailed error messages
        // resources-types set is null
        computer = new Analytics(keyspace, Collections.singleton("thing"), new HashSet<>());
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
        assertIllegalStateExceptionThrown(computer::median);

        // resources-types set is empty
        computer = new Analytics(keyspace, Collections.singleton("thing"), new HashSet<>());
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
        assertIllegalStateExceptionThrown(computer::median);

        // if it's not a resource-type
        computer = new Analytics(keyspace, Collections.singleton("thing"),
                Collections.singleton("thing"));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
        assertIllegalStateExceptionThrown(computer::median);

        // resource-type has no instance
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType7));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());
        assertFalse(computer.median().isPresent());

        // resources are not connected to any entities
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType3));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());
        assertFalse(computer.median().isPresent());

        // resource-type has incorrect data type
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType4));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
        assertIllegalStateExceptionThrown(computer::median);

        // resource-types have different data types
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet(resourceType1, resourceType2));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
        assertIllegalStateExceptionThrown(computer::median);
    }

    private void assertIllegalStateExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.max().isPresent());


        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(thing),
                Collections.singleton(resourceType2));
        assertEquals(-1L, computer.min().get());
        computer = new Analytics(keyspace, Collections.singleton(thing),
                Sets.newHashSet(resourceType2, resourceType5));
        assertEquals(-7L, computer.min().get());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(1.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet(resourceType1, resourceType6));
        assertEquals(7.5, computer.max().get().doubleValue(), delta);
        // TODO: fix this test
        computer = new Analytics(keyspace, Collections.singleton(anotherThing),
                Collections.singleton(resourceType2));
        assertEquals(4L, computer.max().get());

    }

    @Test
    public void testSum() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.sum().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.sum().isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.sum().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.sum().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(4.5, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(3L, computer.sum().get());
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(27.0, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(-18L, computer.sum().get());
    }

    @Test
    public void testMean() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.mean().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.mean().isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.mean().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.mean().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType1)));
        assertEquals(1.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(1D, computer.mean().get(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(4.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(-3D, computer.mean().get(), delta);
    }

    @Test
    public void testStd() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.std().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.std().isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.std().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.std().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType1)));
        assertEquals(Math.sqrt(0.18 / 3), computer.std().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(Math.sqrt(14.0 / 3), computer.std().get(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(Math.sqrt(54.18 / 6), computer.std().get(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(Math.sqrt(110.0 / 6), computer.std().get(), delta);
    }

    @Test
    public void testMedian() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.median().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.median().isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.median().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.median().isPresent());

        // connect entity and resources
        addResourceRelations();

        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType1)));
        assertEquals(1.5D, computer.median().get().doubleValue(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType6)));
        assertEquals(7.5D, computer.median().get().doubleValue(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet(resourceType1, resourceType6));
        assertEquals(1.8D, computer.median().get().doubleValue(), delta);

        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType2)));
        assertEquals(0L, computer.median().get().longValue());
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType5)));
        assertEquals(-7L, computer.median().get().longValue());
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(-7L, computer.median().get().longValue());
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType1.addEntity();
        Entity entity3 = entityType1.addEntity();
        Entity entity4 = entityType2.addEntity();
        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        entityType1.playsRole(relation1).playsRole(relation2);
        entityType2.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        related.addRelation()
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);
        related.addRelation()
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);
        related.addRelation()
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));
        RoleType resourceOwner7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType7));

        RoleType resourceValue1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));
        RoleType resourceValue7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType7));

        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType7))
                .hasRole(resourceOwner7).hasRole(resourceValue7);

        entityType1.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);
        entityType2.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);

        resourceTypeList.forEach(resourceType -> resourceType
                .playsRole(resourceValue1)
                .playsRole(resourceValue2)
                .playsRole(resourceValue3)
                .playsRole(resourceValue4)
                .playsRole(resourceValue5)
                .playsRole(resourceValue6)
                .playsRole(resourceValue7));

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourcesInstances() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        graph.getResourceType(resourceType1).putResource(1.2);
        graph.getResourceType(resourceType1).putResource(1.5);
        graph.getResourceType(resourceType1).putResource(1.8);

        graph.getResourceType(resourceType2).putResource(4L);
        graph.getResourceType(resourceType2).putResource(-1L);
        graph.getResourceType(resourceType2).putResource(0L);

        graph.getResourceType(resourceType5).putResource(6L);
        graph.getResourceType(resourceType5).putResource(7L);
        graph.getResourceType(resourceType5).putResource(8L);

        graph.getResourceType(resourceType6).putResource(7.2);
        graph.getResourceType(resourceType6).putResource(7.5);
        graph.getResourceType(resourceType6).putResource(7.8);

        graph.getResourceType(resourceType4).putResource("a");
        graph.getResourceType(resourceType4).putResource("b");
        graph.getResourceType(resourceType4).putResource("c");

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getConcept(entityId1);
        Entity entity2 = graph.getConcept(entityId2);
        Entity entity3 = graph.getConcept(entityId3);
        Entity entity4 = graph.getConcept(entityId4);

        RoleType resourceOwner1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));

        RelationType relationType1 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.2));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.5));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity3)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.8));

        RelationType relationType2 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(4L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(-1L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(0L));

        graph.getResourceType(resourceType3).putResource(100L);

        RelationType relationType5 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));

        RelationType relationType6 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));

        // some resources in, but not connect them to any instances
        graph.getResourceType(resourceType1).putResource(2.8);
        graph.getResourceType(resourceType2).putResource(-5L);
        graph.getResourceType(resourceType5).putResource(10L);
        graph.getResourceType(resourceType6).putResource(0.8);

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }
}

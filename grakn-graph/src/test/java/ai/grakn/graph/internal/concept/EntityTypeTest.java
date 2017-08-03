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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.concept.TypeImpl;
import ai.grakn.graph.internal.structure.Shard;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.util.ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE;
import static ai.grakn.util.ErrorMessage.RESERVED_WORD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class EntityTypeTest extends GraphTestBase {

    @Before
    public void buildGraph(){
        EntityType top = graknGraph.putEntityType("top");
        EntityType middle1 = graknGraph.putEntityType("mid1");
        EntityType middle2 = graknGraph.putEntityType("mid2");
        EntityType middle3 = graknGraph.putEntityType("mid3'");
        EntityType bottom = graknGraph.putEntityType("bottom");

        bottom.sup(middle1);
        middle1.sup(top);
        middle2.sup(top);
        middle3.sup(top);
    }

    @Test
    public void whenCreatingEntityTypeUsingLabelTakenByAnotherType_Throw(){
        Role original = graknGraph.putRole("Role Type");
        expectedException.expect(PropertyNotUniqueException.class);
        expectedException.expectMessage(PropertyNotUniqueException.cannotCreateProperty(original, Schema.VertexProperty.ONTOLOGY_LABEL, original.getLabel()).getMessage());
        graknGraph.putEntityType(original.getLabel());
    }

    @Test
    public void creatingAccessingDeletingScopes_Works() throws GraphOperationException {
        EntityType entityType = graknGraph.putEntityType("entity type");
        Thing scope1 = entityType.addEntity();
        Thing scope2 = entityType.addEntity();
        Thing scope3 = entityType.addEntity();
        assertThat(entityType.scopes(), is(empty()));

        entityType.scope(scope1);
        entityType.scope(scope2);
        entityType.scope(scope3);
        assertThat(entityType.scopes(), containsInAnyOrder(scope1, scope2, scope3));

        scope1.delete();
        assertThat(entityType.scopes(), containsInAnyOrder(scope2, scope3));

        entityType.deleteScope(scope2);
        assertThat(entityType.scopes(), containsInAnyOrder(scope3));
    }

    @Test
    public void whenDeletingEntityTypeWithSubTypes_Throw() throws GraphOperationException{
        EntityType c1 = graknGraph.putEntityType("C1");
        EntityType c2 = graknGraph.putEntityType("C2");
        c1.sup(c2);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotBeDeleted(c2).getMessage());

        c2.delete();
    }

    @Test
    public void whenGettingTheLabelOfType_TheTypeLabelIsReturned(){
        Type test = graknGraph.putEntityType("test");
        assertEquals(Label.of("test"), test.getLabel());
    }

    @Test
    public void whenGettingTheRolesPlayedByType_ReturnTheRoles() throws Exception{
        Role monster = graknGraph.putRole("monster");
        Role animal = graknGraph.putRole("animal");
        Role monsterEvil = graknGraph.putRole("evil monster").sup(monster);

        EntityType creature = graknGraph.putEntityType("creature").plays(monster).plays(animal);
        EntityType creatureMysterious = graknGraph.putEntityType("mysterious creature").sup(creature).plays(monsterEvil);

        assertThat(creature.plays(), containsInAnyOrder(monster, animal));
        assertThat(creatureMysterious.plays(), containsInAnyOrder(monster, animal, monsterEvil));
    }

    @Test
    public void whenGettingTheSuperSet_ReturnAllOfItsSuperTypes() throws Exception{
        EntityType entityType = graknGraph.admin().getMetaEntityType();
        EntityType c1 = graknGraph.putEntityType("c1");
        EntityType c2 = graknGraph.putEntityType("c2").sup(c1);
        EntityType c3 = graknGraph.putEntityType("c3").sup(c2);
        EntityType c4 = graknGraph.putEntityType("c4").sup(c1);

        Set<EntityType> c1SuperTypes = ((TypeImpl) c1).superSet();
        Set<EntityType> c2SuperTypes = ((TypeImpl) c2).superSet();
        Set<EntityType> c3SuperTypes = ((TypeImpl) c3).superSet();
        Set<EntityType> c4SuperTypes = ((TypeImpl) c4).superSet();

        assertThat(c1SuperTypes, containsInAnyOrder(entityType, c1));
        assertThat(c2SuperTypes, containsInAnyOrder(entityType, c2, c1));
        assertThat(c3SuperTypes, containsInAnyOrder(entityType, c3, c2, c1));
        assertThat(c4SuperTypes, containsInAnyOrder(entityType, c4, c1));
    }

    @Test
    public void whenGettingTheSubTypesOfaType_ReturnAllSubTypes(){
        EntityType parent = graknGraph.putEntityType("parent");
        EntityType c1 = graknGraph.putEntityType("c1").sup(parent);
        EntityType c2 = graknGraph.putEntityType("c2").sup(parent);
        EntityType c3 = graknGraph.putEntityType("c3").sup(c1);

        assertThat(parent.subs(), containsInAnyOrder(parent, c1, c2, c3));
        assertThat(c1.subs(), containsInAnyOrder(c1, c3));
        assertThat(c2.subs(), containsInAnyOrder(c2));
        assertThat(c3.subs(), containsInAnyOrder(c3));
    }

    @Test
    public void whenGettingTheSuperTypeOfType_ReturnSuperType(){
        EntityType c1 = graknGraph.putEntityType("c1");
        EntityType c2 = graknGraph.putEntityType("c2").sup(c1);
        EntityType c3 = graknGraph.putEntityType("c3").sup(c2);

        assertEquals(graknGraph.admin().getMetaEntityType(), c1.sup());
        assertEquals(c1, c2.sup());
        assertEquals(c2, c3.sup());
    }

    @Test
    public void overwriteDefaultSuperTypeWithNewSuperType_ReturnNewSuperType(){
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("A Thing");
        EntityTypeImpl conceptType2 = (EntityTypeImpl) graknGraph.putEntityType("A Super Thing");

        assertEquals(graknGraph.getMetaEntityType(), conceptType.sup());
        conceptType.sup(conceptType2);
        assertEquals(conceptType2, conceptType.sup());
    }

    @Test
    public void whenRemovingRoleFromEntityType_TheRoleCanNoLongerBePlayed(){
        Role role1 = graknGraph.putRole("A Role 1");
        Role role2 = graknGraph.putRole("A Role 2");
        EntityType type = graknGraph.putEntityType("A Concept Type").plays(role1).plays(role2);

        assertThat(type.plays(), containsInAnyOrder(role1, role2));
        type.deletePlays(role1);
        assertThat(type.plays(), containsInAnyOrder( role2));
    }

    @Test
    public void whenGettingTheInstancesOfType_ReturnAllInstances(){
        EntityType e1 = graknGraph.putEntityType("e1");
        EntityType e2 = graknGraph.putEntityType("e2").sup(e1);
        EntityType e3 = graknGraph.putEntityType("e3").sup(e1);

        Entity e2_child1 = e2.addEntity();
        Entity e2_child2 = e2.addEntity();

        Entity e3_child1 = e3.addEntity();
        Entity e3_child2 = e3.addEntity();
        Entity e3_child3 = e3.addEntity();

        assertThat(e1.instances(), containsInAnyOrder(e2_child1, e2_child2, e3_child1, e3_child2, e3_child3));
        assertThat(e2.instances(), containsInAnyOrder(e2_child1, e2_child2));
        assertThat(e3.instances(), containsInAnyOrder(e3_child1, e3_child2, e3_child3));
    }

    @Test
    public void settingTheSuperTypeToItself_Throw(){
        EntityType entityType = graknGraph.putEntityType("Entity");
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.loopCreated(entityType, entityType).getMessage());
        entityType.sup(entityType);
    }

    @Test
    public void whenCyclicSuperTypes_Throw(){
        EntityType entityType1 = graknGraph.putEntityType("Entity1");
        EntityType entityType2 = graknGraph.putEntityType("Entity2");
        EntityType entityType3 = graknGraph.putEntityType("Entity3");
        entityType1.sup(entityType2);
        entityType2.sup(entityType3);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.loopCreated(entityType3, entityType1).getMessage());

        entityType3.sup(entityType1);
    }

    @Test
    public void whenSettingMetaTypeToAbstract_Throw(){
        Type meta = graknGraph.getMetaRuleType();

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.metaTypeImmutable(meta.getLabel()).getMessage());

        meta.setAbstract(true);
    }

    @Test
    public void whenAddingRoleToMetaType_Throw(){
        Type meta = graknGraph.getMetaRuleType();
        Role role = graknGraph.putRole("A Role");

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.metaTypeImmutable(meta.getLabel()).getMessage());

        meta.plays(role);
    }

    @Test
    public void whenAddingResourcesWithSubTypesToEntityTypes_EnsureImplicitStructureFollowsSubTypes(){
        EntityType entityType1 = graknGraph.putEntityType("Entity Type 1");
        EntityType entityType2 = graknGraph.putEntityType("Entity Type 2");

        Label superLabel = Label.of("Super Resource Type");
        Label label = Label.of("Resource Type");

        ResourceType rtSuper = graknGraph.putResourceType(superLabel, ResourceType.DataType.STRING);
        ResourceType rt = graknGraph.putResourceType(label, ResourceType.DataType.STRING).sup(rtSuper);

        entityType1.resource(rtSuper);
        entityType2.resource(rt);

        //Check role types are only built explicitly
        assertThat(entityType1.plays(),
                containsInAnyOrder(graknGraph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(superLabel).getValue())));

        assertThat(entityType2.plays(),
                containsInAnyOrder(graknGraph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(label).getValue())));

        //Check Implicit Types Follow SUB Structure
        RelationType rtSuperRelation = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(rtSuper.getLabel()));
        Role rtSuperRoleOwner = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(rtSuper.getLabel()));
        Role rtSuperRoleValue = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(rtSuper.getLabel()));

        RelationType rtRelation = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(rt.getLabel()));
        Role reRoleOwner = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(rt.getLabel()));
        Role reRoleValue = graknGraph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(rt.getLabel()));

        assertEquals(rtSuperRoleOwner, reRoleOwner.sup());
        assertEquals(rtSuperRoleValue, reRoleValue.sup());
        assertEquals(rtSuperRelation, rtRelation.sup());
    }

    @Test
    public void whenDeletingTypeWithEntities_Throw(){
        EntityType entityTypeA = graknGraph.putEntityType("entityTypeA");
        EntityType entityTypeB = graknGraph.putEntityType("entityTypeB");

        entityTypeB.addEntity();

        entityTypeA.delete();
        assertNull(graknGraph.getEntityType("entityTypeA"));

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotBeDeleted(entityTypeB).getMessage());

        entityTypeB.delete();
    }

    @Test
    public void whenChangingSuperTypeBackToMetaType_EnsureTypeIsResetToMeta(){
        EntityType entityTypeA = graknGraph.putEntityType("entityTypeA");
        EntityType entityTypeB = graknGraph.putEntityType("entityTypeB").sup(entityTypeA);
        assertEquals(entityTypeA, entityTypeB.sup());

        //Making sure put does not effect super type
        entityTypeB = graknGraph.putEntityType("entityTypeB");
        assertEquals(entityTypeA, entityTypeB.sup());

        //Changing super type back to meta explicitly
        entityTypeB.sup(graknGraph.getMetaEntityType());
        assertEquals(graknGraph.getMetaEntityType(), entityTypeB.sup());

    }

    @Test
    public void checkSubTypeCachingUpdatedCorrectlyWhenChangingSuperTypes(){
        EntityType e1 = graknGraph.putEntityType("entityType1");
        EntityType e2 = graknGraph.putEntityType("entityType2").sup(e1);
        EntityType e3 = graknGraph.putEntityType("entityType3").sup(e1);
        EntityType e4 = graknGraph.putEntityType("entityType4").sup(e1);
        EntityType e5 = graknGraph.putEntityType("entityType5");
        EntityType e6 = graknGraph.putEntityType("entityType6").sup(e5);

        assertThat(e1.subs(), containsInAnyOrder(e1, e2, e3, e4));
        assertThat(e5.subs(), containsInAnyOrder(e6, e5));

        //Now change subtypes
        e6.sup(e1);
        e3.sup(e5);

        assertThat(e1.subs(), containsInAnyOrder(e1, e2, e4, e6));
        assertThat(e5.subs(), containsInAnyOrder(e3, e5));
    }

    @Test
    public void checkThatResourceTypesCanBeRetrievedFromTypes(){
        EntityType e1 = graknGraph.putEntityType("e1");
        ResourceType r1 = graknGraph.putResourceType("r1", ResourceType.DataType.STRING);
        ResourceType r2 = graknGraph.putResourceType("r2", ResourceType.DataType.LONG);
        ResourceType r3 = graknGraph.putResourceType("r3", ResourceType.DataType.BOOLEAN);

        assertTrue("Entity is linked to resources when it shouldn't", e1.resources().isEmpty());
        e1.resource(r1);
        e1.resource(r2);
        e1.resource(r3);
        assertThat(e1.resources(), containsInAnyOrder(r1, r2, r3));
    }

    @Test
    public void addResourceTypeAsKeyToOneEntityTypeAndAsResourceToAnotherEntityType(){
        ResourceType<String> resourceType1 = graknGraph.putResourceType("Shared Resource 1", ResourceType.DataType.STRING);
        ResourceType<String> resourceType2 = graknGraph.putResourceType("Shared Resource 2", ResourceType.DataType.STRING);

        EntityType entityType1 = graknGraph.putEntityType("EntityType 1");
        EntityType entityType2 = graknGraph.putEntityType("EntityType 2");

        assertThat(entityType1.keys(), is(empty()));
        assertThat(entityType1.resources(), is(empty()));
        assertThat(entityType2.keys(), is(empty()));
        assertThat(entityType2.resources(), is(empty()));

        //Link the resources
        entityType1.resource(resourceType1);

        entityType1.key(resourceType2);
        entityType2.key(resourceType1);
        entityType2.key(resourceType2);

        assertThat(entityType1.resources(), containsInAnyOrder(resourceType1, resourceType2));
        assertThat(entityType2.resources(), containsInAnyOrder(resourceType1, resourceType2));

        assertThat(entityType1.keys(), containsInAnyOrder(resourceType2));
        assertThat(entityType2.keys(), containsInAnyOrder(resourceType1, resourceType2));

        //Add resource which is a key for one entity and a resource for another
        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType2.addEntity();
        Resource<String> resource1 = resourceType1.putResource("Test 1");
        Resource<String> resource2 = resourceType2.putResource("Test 2");
        Resource<String> resource3 = resourceType2.putResource("Test 3");

        //Resource 1 is a key to one and a resource to another
        entity1.resource(resource1);
        entity2.resource(resource1);

        entity1.resource(resource2);
        entity2.resource(resource3);

        graknGraph.commit();
    }

    @Test
    public void whenAddingResourceTypeAsKeyAfterResource_Throw(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Shared Resource", ResourceType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("EntityType");

        entityType.resource(resourceType);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), resourceType.getLabel()));

        entityType.key(resourceType);
    }

    @Test
    public void whenAddingResourceTypeAsResourceAfterResource_Throw(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Shared Resource", ResourceType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("EntityType");

        entityType.key(resourceType);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), resourceType.getLabel()));

        entityType.resource(resourceType);
    }

    @Test
    public void whenCreatingEntityType_EnsureItHasAShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) graknGraph.putEntityType("EntityType");
        assertThat(entityType.shards(), not(empty()));
        assertEquals(entityType.shards().iterator().next(), entityType.currentShard());
    }

    @Test
    public void whenAddingInstanceToType_EnsureIsaEdgeIsPlacedOnShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) graknGraph.putEntityType("EntityType");
        Shard shard =  entityType.currentShard();
        Entity e1 = entityType.addEntity();

        assertFalse("The isa edge was places on the type rather than the shard", entityType.neighbours(Direction.IN, Schema.EdgeLabel.ISA).iterator().hasNext());
        assertEquals(e1, shard.links().findAny().get());
    }

    @Test
    public void whenAddingTypeUsingReservedWord_ThrowReadableError(){
        String reservedWord = Schema.MetaSchema.THING.getLabel().getValue();

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(RESERVED_WORD.getMessage(reservedWord));

        graknGraph.putEntityType(reservedWord);
    }

}
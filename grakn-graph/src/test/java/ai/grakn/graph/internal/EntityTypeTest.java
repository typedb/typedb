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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.util.ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE;
import static ai.grakn.util.ErrorMessage.CANNOT_DELETE;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class EntityTypeTest extends GraphTestBase{

    @Before
    public void buildGraph(){
        EntityType top = graknGraph.putEntityType("top");
        EntityType middle1 = graknGraph.putEntityType("mid1");
        EntityType middle2 = graknGraph.putEntityType("mid2");
        EntityType middle3 = graknGraph.putEntityType("mid3'");
        EntityType bottom = graknGraph.putEntityType("bottom");

        bottom.superType(middle1);
        middle1.superType(top);
        middle2.superType(top);
        middle3.superType(top);
    }

    @Test
    public void creatingAccessingDeletingScopes_Works() throws ConceptException {
        EntityType entityType = graknGraph.putEntityType("entity type");
        Instance scope1 = entityType.addEntity();
        Instance scope2 = entityType.addEntity();
        Instance scope3 = entityType.addEntity();
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
    public void whenDeletingEntityTypeWithSubTypes_Throw() throws ConceptException{
        EntityType c1 = graknGraph.putEntityType("C1");
        EntityType c2 = graknGraph.putEntityType("C2");
        c1.superType(c2);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.CANNOT_DELETE.getMessage(c2.getLabel()));

        c2.delete();
    }

    @Test
    public void whenGettingTheLabelOfType_TheTypeLabelIsReturned(){
        Type test = graknGraph.putEntityType("test");
        assertEquals(TypeLabel.of("test"), test.getLabel());
    }

    @Test
    public void whenGettingARoleTypeAsType_TheTypeIsReturned(){
        RoleType test1 = graknGraph.putRoleType("test");
        Type test2 = graknGraph.getType(TypeLabel.of("test"));
        assertEquals(test1, test2);
    }

    @Test
    public void whenGettingTheRolesPlayedByType_ReturnTheRoles() throws Exception{
        RoleType monster = graknGraph.putRoleType("monster");
        RoleType animal = graknGraph.putRoleType("animal");
        RoleType monsterEvil = graknGraph.putRoleType("evil monster").superType(monster);

        EntityType creature = graknGraph.putEntityType("creature").plays(monster).plays(animal);
        EntityType creatureMysterious = graknGraph.putEntityType("mysterious creature").superType(creature).plays(monsterEvil);

        assertThat(creature.plays(), containsInAnyOrder(monster, animal));
        assertThat(creatureMysterious.plays(), containsInAnyOrder(monster, animal, monsterEvil));
    }

    @Test
    public void whenGettingTheSuperSet_ReturnAllOfItsSuperTypes() throws Exception{
        EntityType entityType = graknGraph.admin().getMetaEntityType();
        EntityType c1 = graknGraph.putEntityType("c1");
        EntityType c2 = graknGraph.putEntityType("c2").superType(c1);
        EntityType c3 = graknGraph.putEntityType("c3").superType(c2);
        EntityType c4 = graknGraph.putEntityType("c4").superType(c1);

        Set<EntityType> c1SuperTypes = ((TypeImpl) c1).superTypeSet();
        Set<EntityType> c2SuperTypes = ((TypeImpl) c2).superTypeSet();
        Set<EntityType> c3SuperTypes = ((TypeImpl) c3).superTypeSet();
        Set<EntityType> c4SuperTypes = ((TypeImpl) c4).superTypeSet();

        assertThat(c1SuperTypes, containsInAnyOrder(entityType, c1));
        assertThat(c2SuperTypes, containsInAnyOrder(entityType, c2, c1));
        assertThat(c3SuperTypes, containsInAnyOrder(entityType, c3, c2, c1));
        assertThat(c4SuperTypes, containsInAnyOrder(entityType, c4, c1));
    }

    @Test
    public void whenGettingTheSubTypesOfaType_ReturnAllSubTypes(){
        EntityType parent = graknGraph.putEntityType("parent");
        EntityType c1 = graknGraph.putEntityType("c1").superType(parent);
        EntityType c2 = graknGraph.putEntityType("c2").superType(parent);
        EntityType c3 = graknGraph.putEntityType("c3").superType(c1);

        assertThat(parent.subTypes(), containsInAnyOrder(parent, c1, c2, c3));
        assertThat(c1.subTypes(), containsInAnyOrder(c1, c3));
        assertThat(c2.subTypes(), containsInAnyOrder(c2));
        assertThat(c3.subTypes(), containsInAnyOrder(c3));
    }

    @Test
    public void whenGettingTheSuperTypeOfType_ReturnSuperType(){
        EntityType c1 = graknGraph.putEntityType("c1");
        EntityType c2 = graknGraph.putEntityType("c2").superType(c1);
        EntityType c3 = graknGraph.putEntityType("c3").superType(c2);

        assertEquals(graknGraph.admin().getMetaEntityType(), c1.superType());
        assertEquals(c1, c2.superType());
        assertEquals(c2, c3.superType());
    }

    @Test
    public void overwriteDefaultSuperTypeWithNewSuperType_ReturnNewSuperType(){
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("A Thing");
        EntityTypeImpl conceptType2 = (EntityTypeImpl) graknGraph.putEntityType("A Super Thing");

        assertEquals(graknGraph.getMetaEntityType(), conceptType.superType());
        conceptType.superType(conceptType2);
        assertEquals(conceptType2, conceptType.superType());
    }

    @Test
    public void whenRemovingRoleFromEntityType_TheRoleCanNoLongerBePlayed(){
        RoleType role1 = graknGraph.putRoleType("A Role 1");
        RoleType role2 = graknGraph.putRoleType("A Role 2");
        EntityType type = graknGraph.putEntityType("A Concept Type").plays(role1).plays(role2);

        assertThat(type.plays(), containsInAnyOrder(role1, role2));
        type.deletePlays(role1);
        assertThat(type.plays(), containsInAnyOrder( role2));
    }

    @Test
    public void whenGettingTheInstancesOfType_ReturnAllInstances(){
        EntityType e1 = graknGraph.putEntityType("e1");
        EntityType e2 = graknGraph.putEntityType("e2").superType(e1);
        EntityType e3 = graknGraph.putEntityType("e3").superType(e1);

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
        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.SUPER_TYPE_LOOP_DETECTED.getMessage(entityType.getLabel(), entityType.getLabel()));
        entityType.superType(entityType);
    }

    @Test
    public void whenCyclicSuperTypes_Throw(){
        EntityType entityType1 = graknGraph.putEntityType("Entity1");
        EntityType entityType2 = graknGraph.putEntityType("Entity2");
        EntityType entityType3 = graknGraph.putEntityType("Entity3");
        entityType1.superType(entityType2);
        entityType2.superType(entityType3);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(ErrorMessage.SUPER_TYPE_LOOP_DETECTED.getMessage(entityType3.getLabel(), entityType1.getLabel()));

        entityType3.superType(entityType1);
    }

    @Test
    public void whenSettingMetaTypeToAbstract_Throw(){
        Type meta = graknGraph.getMetaRuleType();

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(META_TYPE_IMMUTABLE.getMessage(meta.getLabel()));

        meta.setAbstract(true);
    }

    @Test
    public void whenAddingRoleToMetaType_Throw(){
        Type meta = graknGraph.getMetaRuleType();
        RoleType roleType = graknGraph.putRoleType("A Role");

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(META_TYPE_IMMUTABLE.getMessage(meta.getLabel()));

        meta.plays(roleType);
    }

    @Test
    public void whenSpecifyingTheResourceTypeOfAnEntityType_EnsureTheImplicitStructureIsCreated(){
        graknGraph.showImplicitConcepts(true);
        TypeLabel resourceTypeLabel = TypeLabel.of("Resource Type");
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        //Implicit Names
        TypeLabel hasResourceOwnerLabel = Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeLabel);
        TypeLabel hasResourceValueLabel = Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeLabel);
        TypeLabel hasResourceLabel = Schema.ImplicitType.HAS.getLabel(resourceTypeLabel);

        entityType.resource(resourceType);

        RelationType relationType = graknGraph.getRelationType(hasResourceLabel.getValue());
        assertEquals(hasResourceLabel, relationType.getLabel());

        Set<TypeLabel> roleLabels = relationType.relates().stream().map(Type::getLabel).collect(toSet());
        assertThat(roleLabels, containsInAnyOrder(hasResourceOwnerLabel, hasResourceValueLabel));

        assertThat(entityType.plays(), containsInAnyOrder(graknGraph.getRoleType(hasResourceOwnerLabel.getValue())));
        assertThat(resourceType.plays(), containsInAnyOrder(graknGraph.getRoleType(hasResourceValueLabel.getValue())));

        //Check everything is implicit
        assertTrue(relationType.isImplicit());
        relationType.relates().forEach(role -> assertTrue(role.isImplicit()));

        // Check that resource is not required
        EdgeImpl entityPlays = ((EntityTypeImpl) entityType).getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(entityPlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeImpl resourcePlays = ((ResourceTypeImpl <?>) resourceType).getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(resourcePlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void whenAddingResourcesWithSubTypesToEntityTypes_EnsureImplicitStructureFollowsSubTypes(){
        EntityType entityType1 = graknGraph.putEntityType("Entity Type 1");
        EntityType entityType2 = graknGraph.putEntityType("Entity Type 2");

        TypeLabel superLabel = TypeLabel.of("Super Resource Type");
        TypeLabel label = TypeLabel.of("Resource Type");

        ResourceType rtSuper = graknGraph.putResourceType(superLabel, ResourceType.DataType.STRING);
        ResourceType rt = graknGraph.putResourceType(label, ResourceType.DataType.STRING).superType(rtSuper);

        entityType1.resource(rtSuper);
        entityType2.resource(rt);

        graknGraph.showImplicitConcepts(true);

        //Check role types are only built explicitly
        assertThat(entityType1.plays(),
                containsInAnyOrder(graknGraph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(superLabel).getValue())));

        assertThat(entityType2.plays(),
                containsInAnyOrder(graknGraph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(label).getValue())));

        //Check Implicit Types Follow SUB Structure
        RelationType rtSuperRelation = graknGraph.getType(Schema.ImplicitType.HAS.getLabel(rtSuper.getLabel()));
        RoleType rtSuperRoleOwner = graknGraph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(rtSuper.getLabel()));
        RoleType rtSuperRoleValue = graknGraph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(rtSuper.getLabel()));

        RelationType rtRelation = graknGraph.getType(Schema.ImplicitType.HAS.getLabel(rt.getLabel()));
        RoleType reRoleOwner = graknGraph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(rt.getLabel()));
        RoleType reRoleValue = graknGraph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(rt.getLabel()));

        assertEquals(rtSuperRoleOwner, reRoleOwner.superType());
        assertEquals(rtSuperRoleValue, reRoleValue.superType());
        assertEquals(rtSuperRelation, rtRelation.superType());
    }

    @Test
    public void whenDeletingTypeWithEntities_Throw(){
        EntityType entityTypeA = graknGraph.putEntityType("entityTypeA");
        EntityType entityTypeB = graknGraph.putEntityType("entityTypeB");

        entityTypeB.addEntity();

        entityTypeA.delete();
        assertNull(graknGraph.getEntityType("entityTypeA"));

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(CANNOT_DELETE.getMessage(entityTypeB.getLabel()));

        entityTypeB.delete();
    }

    @Test
    public void whenChangingSuperTypeBackToMetaType_EnsureTypeIsResetToMeta(){
        EntityType entityTypeA = graknGraph.putEntityType("entityTypeA");
        EntityType entityTypeB = graknGraph.putEntityType("entityTypeB").superType(entityTypeA);
        assertEquals(entityTypeA, entityTypeB.superType());

        //Making sure put does not effect super type
        entityTypeB = graknGraph.putEntityType("entityTypeB");
        assertEquals(entityTypeA, entityTypeB.superType());

        //Changing super type back to meta explicitly
        entityTypeB.superType(graknGraph.getMetaEntityType());
        assertEquals(graknGraph.getMetaEntityType(), entityTypeB.superType());

    }

    @Test
    public void checkSubTypeCachingUpdatedCorrectlyWhenChangingSuperTypes(){
        EntityType e1 = graknGraph.putEntityType("entityType1");
        EntityType e2 = graknGraph.putEntityType("entityType2").superType(e1);
        EntityType e3 = graknGraph.putEntityType("entityType3").superType(e1);
        EntityType e4 = graknGraph.putEntityType("entityType4").superType(e1);
        EntityType e5 = graknGraph.putEntityType("entityType5");
        EntityType e6 = graknGraph.putEntityType("entityType6").superType(e5);

        assertThat(e1.subTypes(), containsInAnyOrder(e1, e2, e3, e4));
        assertThat(e5.subTypes(), containsInAnyOrder(e6, e5));

        //Now change subtypes
        e6.superType(e1);
        e3.superType(e5);

        assertThat(e1.subTypes(), containsInAnyOrder(e1, e2, e4, e6));
        assertThat(e5.subTypes(), containsInAnyOrder(e3, e5));
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

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), resourceType.getLabel()));

        entityType.key(resourceType);
    }

    @Test
    public void whenAddingResourceTypeAsResourceAfterResource_Throw(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Shared Resource", ResourceType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("EntityType");

        entityType.key(resourceType);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), resourceType.getLabel()));

        entityType.resource(resourceType);
    }

}
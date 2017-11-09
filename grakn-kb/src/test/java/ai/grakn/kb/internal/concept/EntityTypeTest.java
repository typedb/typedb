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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.kb.internal.structure.Shard;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE;
import static ai.grakn.util.ErrorMessage.RESERVED_WORD;
import static java.util.stream.Collectors.toSet;
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
public class EntityTypeTest extends TxTestBase {

    @Before
    public void setup(){
        EntityType top = tx.putEntityType("top");
        EntityType middle1 = tx.putEntityType("mid1");
        EntityType middle2 = tx.putEntityType("mid2");
        EntityType middle3 = tx.putEntityType("mid3'");
        EntityType bottom = tx.putEntityType("bottom");

        bottom.sup(middle1);
        middle1.sup(top);
        middle2.sup(top);
        middle3.sup(top);
    }

    @Test
    public void whenCreatingEntityTypeUsingLabelTakenByAnotherType_Throw(){
        Role original = tx.putRole("Role Type");
        expectedException.expect(PropertyNotUniqueException.class);
        expectedException.expectMessage(PropertyNotUniqueException.cannotCreateProperty(original, Schema.VertexProperty.SCHEMA_LABEL, original.getLabel()).getMessage());
        tx.putEntityType(original.getLabel());
    }

    @Test
    public void whenDeletingEntityTypeWithSubTypes_Throw() throws GraknTxOperationException {
        EntityType c1 = tx.putEntityType("C1");
        EntityType c2 = tx.putEntityType("C2");
        c1.sup(c2);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(c2).getMessage());

        c2.delete();
    }

    @Test
    public void whenGettingTheLabelOfType_TheTypeLabelIsReturned(){
        Type test = tx.putEntityType("test");
        assertEquals(Label.of("test"), test.getLabel());
    }

    @Test
    public void whenGettingTheRolesPlayedByType_ReturnTheRoles() throws Exception{
        Role monster = tx.putRole("monster");
        Role animal = tx.putRole("animal");
        Role monsterEvil = tx.putRole("evil monster").sup(monster);

        EntityType creature = tx.putEntityType("creature").plays(monster).plays(animal);
        EntityType creatureMysterious = tx.putEntityType("mysterious creature").sup(creature).plays(monsterEvil);

        assertThat(creature.plays().collect(toSet()), containsInAnyOrder(monster, animal));
        assertThat(creatureMysterious.plays().collect(toSet()), containsInAnyOrder(monster, animal, monsterEvil));
    }

    @Test
    public void whenGettingTheSuperSet_ReturnAllOfItsSuperTypes() throws Exception{
        EntityType entityType = tx.admin().getMetaEntityType();
        EntityType c1 = tx.putEntityType("c1");
        EntityType c2 = tx.putEntityType("c2").sup(c1);
        EntityType c3 = tx.putEntityType("c3").sup(c2);
        EntityType c4 = tx.putEntityType("c4").sup(c1);

        Set<Type> c1SuperTypes = EntityTypeImpl.from(c1).superSet().collect(toSet());
        Set<Type> c2SuperTypes = EntityTypeImpl.from(c2).superSet().collect(toSet());
        Set<Type> c3SuperTypes = EntityTypeImpl.from(c3).superSet().collect(toSet());
        Set<Type> c4SuperTypes = EntityTypeImpl.from(c4).superSet().collect(toSet());

        assertThat(c1SuperTypes, containsInAnyOrder(entityType, c1));
        assertThat(c2SuperTypes, containsInAnyOrder(entityType, c2, c1));
        assertThat(c3SuperTypes, containsInAnyOrder(entityType, c3, c2, c1));
        assertThat(c4SuperTypes, containsInAnyOrder(entityType, c4, c1));
    }

    @Test
    public void whenGettingTheSuperSetViaSupsMethod_ReturnAllOfItsSuperTypes(){
        EntityType child = tx.putEntityType("child");
        EntityType p2 = tx.putEntityType("p2").sub(child);
        EntityType p3 = tx.putEntityType("p3").sub(p2);
        EntityType p4 = tx.putEntityType("p4").sub(p3);

        assertThat(child.sups().collect(toSet()), containsInAnyOrder(child, p2, p3, p4));
        assertThat(p2.sups().collect(toSet()), containsInAnyOrder(p2,p3, p4));
        assertThat(p3.sups().collect(toSet()), containsInAnyOrder(p3,p4));
        assertThat(p4.sups().collect(toSet()), containsInAnyOrder(p4));

    }

    @Test
    public void whenGettingTheSubTypesOfaType_ReturnAllSubTypes(){
        EntityType parent = tx.putEntityType("parent");
        EntityType c1 = tx.putEntityType("c1").sup(parent);
        EntityType c2 = tx.putEntityType("c2").sup(parent);
        EntityType c3 = tx.putEntityType("c3").sup(c1);

        assertThat(parent.subs().collect(toSet()), containsInAnyOrder(parent, c1, c2, c3));
        assertThat(c1.subs().collect(toSet()), containsInAnyOrder(c1, c3));
        assertThat(c2.subs().collect(toSet()), containsInAnyOrder(c2));
        assertThat(c3.subs().collect(toSet()), containsInAnyOrder(c3));
    }

    @Test
    public void whenGettingTheSuperTypeOfType_ReturnSuperType(){
        EntityType c1 = tx.putEntityType("c1");
        EntityType c2 = tx.putEntityType("c2").sup(c1);
        EntityType c3 = tx.putEntityType("c3").sup(c2);

        assertEquals(tx.admin().getMetaEntityType(), c1.sup());
        assertEquals(c1, c2.sup());
        assertEquals(c2, c3.sup());
    }

    @Test
    public void overwriteDefaultSuperTypeWithNewSuperType_ReturnNewSuperType(){
        EntityTypeImpl conceptType = (EntityTypeImpl) tx.putEntityType("A Thing");
        EntityTypeImpl conceptType2 = (EntityTypeImpl) tx.putEntityType("A Super Thing");

        assertEquals(tx.getMetaEntityType(), conceptType.sup());
        conceptType.sup(conceptType2);
        assertEquals(conceptType2, conceptType.sup());
    }

    @Test
    public void whenRemovingRoleFromEntityType_TheRoleCanNoLongerBePlayed(){
        Role role1 = tx.putRole("A Role 1");
        Role role2 = tx.putRole("A Role 2");
        EntityType type = tx.putEntityType("A Concept Type").plays(role1).plays(role2);

        assertThat(type.plays().collect(toSet()), containsInAnyOrder(role1, role2));
        type.deletePlays(role1);
        assertThat(type.plays().collect(toSet()), containsInAnyOrder( role2));
    }

    @Test
    public void whenGettingTheInstancesOfType_ReturnAllInstances(){
        EntityType e1 = tx.putEntityType("e1");
        EntityType e2 = tx.putEntityType("e2").sup(e1);
        EntityType e3 = tx.putEntityType("e3").sup(e1);

        Entity e2_child1 = e2.addEntity();
        Entity e2_child2 = e2.addEntity();

        Entity e3_child1 = e3.addEntity();
        Entity e3_child2 = e3.addEntity();
        Entity e3_child3 = e3.addEntity();

        assertThat(e1.instances().collect(toSet()), containsInAnyOrder(e2_child1, e2_child2, e3_child1, e3_child2, e3_child3));
        assertThat(e2.instances().collect(toSet()), containsInAnyOrder(e2_child1, e2_child2));
        assertThat(e3.instances().collect(toSet()), containsInAnyOrder(e3_child1, e3_child2, e3_child3));
    }

    @Test
    public void settingTheSuperTypeToItself_Throw(){
        EntityType entityType = tx.putEntityType("Entity");
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.loopCreated(entityType, entityType).getMessage());
        entityType.sup(entityType);
    }

    @Test
    public void whenCyclicSuperTypes_Throw(){
        EntityType entityType1 = tx.putEntityType("Entity1");
        EntityType entityType2 = tx.putEntityType("Entity2");
        EntityType entityType3 = tx.putEntityType("Entity3");
        entityType1.sup(entityType2);
        entityType2.sup(entityType3);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.loopCreated(entityType3, entityType1).getMessage());

        entityType3.sup(entityType1);
    }

    @Test
    public void whenSettingMetaTypeToAbstract_Throw(){
        Type meta = tx.getMetaEntityType();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.metaTypeImmutable(meta.getLabel()).getMessage());

        meta.setAbstract(true);
    }

    @Test
    public void whenAddingRoleToMetaType_Throw(){
        Type meta = tx.getMetaEntityType();
        Role role = tx.putRole("A Role");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.metaTypeImmutable(meta.getLabel()).getMessage());

        meta.plays(role);
    }

    @Test
    public void whenAddingResourcesWithSubTypesToEntityTypes_EnsureImplicitStructureFollowsSubTypes(){
        EntityType entityType1 = tx.putEntityType("Entity Type 1");
        EntityType entityType2 = tx.putEntityType("Entity Type 2");

        Label superLabel = Label.of("Super Attribute Type");
        Label label = Label.of("Attribute Type");

        AttributeType rtSuper = tx.putAttributeType(superLabel, AttributeType.DataType.STRING);
        AttributeType rt = tx.putAttributeType(label, AttributeType.DataType.STRING).sup(rtSuper);

        entityType1.attribute(rtSuper);
        entityType2.attribute(rt);

        //Check role types are only built explicitly
        assertThat(entityType1.plays().collect(toSet()),
                containsInAnyOrder(tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(superLabel).getValue())));

        assertThat(entityType2.plays().collect(toSet()),
                containsInAnyOrder(tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(label).getValue())));

        //Check Implicit Types Follow SUB Structure
        RelationshipType rtSuperRelation = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(rtSuper.getLabel()));
        Role rtSuperRoleOwner = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(rtSuper.getLabel()));
        Role rtSuperRoleValue = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(rtSuper.getLabel()));

        RelationshipType rtRelation = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(rt.getLabel()));
        Role reRoleOwner = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(rt.getLabel()));
        Role reRoleValue = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(rt.getLabel()));

        assertEquals(rtSuperRoleOwner, reRoleOwner.sup());
        assertEquals(rtSuperRoleValue, reRoleValue.sup());
        assertEquals(rtSuperRelation, rtRelation.sup());
    }

    @Test
    public void whenDeletingTypeWithEntities_Throw(){
        EntityType entityTypeA = tx.putEntityType("entityTypeA");
        EntityType entityTypeB = tx.putEntityType("entityTypeB");

        entityTypeB.addEntity();

        entityTypeA.delete();
        assertNull(tx.getEntityType("entityTypeA"));

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(entityTypeB).getMessage());

        entityTypeB.delete();
    }

    @Test
    public void whenChangingSuperTypeBackToMetaType_EnsureTypeIsResetToMeta(){
        EntityType entityTypeA = tx.putEntityType("entityTypeA");
        EntityType entityTypeB = tx.putEntityType("entityTypeB").sup(entityTypeA);
        assertEquals(entityTypeA, entityTypeB.sup());

        //Making sure put does not effect super type
        entityTypeB = tx.putEntityType("entityTypeB");
        assertEquals(entityTypeA, entityTypeB.sup());

        //Changing super type back to meta explicitly
        entityTypeB.sup(tx.getMetaEntityType());
        assertEquals(tx.getMetaEntityType(), entityTypeB.sup());

    }

    @Test
    public void checkSubTypeCachingUpdatedCorrectlyWhenChangingSuperTypes(){
        EntityType e1 = tx.putEntityType("entityType1");
        EntityType e2 = tx.putEntityType("entityType2").sup(e1);
        EntityType e3 = tx.putEntityType("entityType3").sup(e1);
        EntityType e4 = tx.putEntityType("entityType4").sup(e1);
        EntityType e5 = tx.putEntityType("entityType5");
        EntityType e6 = tx.putEntityType("entityType6").sup(e5);

        assertThat(e1.subs().collect(toSet()), containsInAnyOrder(e1, e2, e3, e4));
        assertThat(e5.subs().collect(toSet()), containsInAnyOrder(e6, e5));

        //Now change subtypes
        e6.sup(e1);
        e3.sup(e5);

        assertThat(e1.subs().collect(toSet()), containsInAnyOrder(e1, e2, e4, e6));
        assertThat(e5.subs().collect(toSet()), containsInAnyOrder(e3, e5));
    }

    @Test
    public void checkThatResourceTypesCanBeRetrievedFromTypes(){
        EntityType e1 = tx.putEntityType("e1");
        AttributeType r1 = tx.putAttributeType("r1", AttributeType.DataType.STRING);
        AttributeType r2 = tx.putAttributeType("r2", AttributeType.DataType.LONG);
        AttributeType r3 = tx.putAttributeType("r3", AttributeType.DataType.BOOLEAN);

        assertTrue("Entity is linked to resources when it shouldn't", e1.attributes().collect(toSet()).isEmpty());
        e1.attribute(r1);
        e1.attribute(r2);
        e1.attribute(r3);
        assertThat(e1.attributes().collect(toSet()), containsInAnyOrder(r1, r2, r3));
    }

    @Test
    public void addResourceTypeAsKeyToOneEntityTypeAndAsResourceToAnotherEntityType(){
        AttributeType<String> attributeType1 = tx.putAttributeType("Shared Attribute 1", AttributeType.DataType.STRING);
        AttributeType<String> attributeType2 = tx.putAttributeType("Shared Attribute 2", AttributeType.DataType.STRING);

        EntityType entityType1 = tx.putEntityType("EntityType 1");
        EntityType entityType2 = tx.putEntityType("EntityType 2");

        assertThat(entityType1.keys().collect(toSet()), is(empty()));
        assertThat(entityType1.attributes().collect(toSet()), is(empty()));
        assertThat(entityType2.keys().collect(toSet()), is(empty()));
        assertThat(entityType2.attributes().collect(toSet()), is(empty()));

        //Link the resources
        entityType1.attribute(attributeType1);

        entityType1.key(attributeType2);
        entityType2.key(attributeType1);
        entityType2.key(attributeType2);

        assertThat(entityType1.attributes().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));
        assertThat(entityType2.attributes().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));

        assertThat(entityType1.keys().collect(toSet()), containsInAnyOrder(attributeType2));
        assertThat(entityType2.keys().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));

        //Add resource which is a key for one entity and a resource for another
        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType2.addEntity();
        Attribute<String> attribute1 = attributeType1.putAttribute("Test 1");
        Attribute<String> attribute2 = attributeType2.putAttribute("Test 2");
        Attribute<String> attribute3 = attributeType2.putAttribute("Test 3");

        //Attribute 1 is a key to one and a resource to another
        entity1.attribute(attribute1);
        entity2.attribute(attribute1);

        entity1.attribute(attribute2);
        entity2.attribute(attribute3);

        tx.commit();
    }

    @Test
    public void whenAddingResourceTypeAsKeyAfterResource_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("Shared Attribute", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("EntityType");

        entityType.attribute(attributeType);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), attributeType.getLabel()));

        entityType.key(attributeType);
    }

    @Test
    public void whenAddingResourceTypeAsResourceAfterResource_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("Shared Attribute", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("EntityType");

        entityType.key(attributeType);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_RESOURCE.getMessage(entityType.getLabel(), attributeType.getLabel()));

        entityType.attribute(attributeType);
    }

    @Test
    public void whenCreatingEntityType_EnsureItHasAShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) tx.putEntityType("EntityType");
        assertThat(entityType.shards().collect(toSet()), not(empty()));
        assertEquals(entityType.shards().iterator().next(), entityType.currentShard());
    }

    @Test
    public void whenAddingInstanceToType_EnsureIsaEdgeIsPlacedOnShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) tx.putEntityType("EntityType");
        Shard shard =  entityType.currentShard();
        Entity e1 = entityType.addEntity();

        assertFalse("The isa edge was places on the type rather than the shard", entityType.neighbours(Direction.IN, Schema.EdgeLabel.ISA).iterator().hasNext());
        assertEquals(e1, shard.links().findAny().get());
    }

    @Test
    public void whenAddingTypeUsingReservedWord_ThrowReadableError(){
        String reservedWord = Schema.MetaSchema.THING.getLabel().getValue();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(RESERVED_WORD.getMessage(reservedWord));

        tx.putEntityType(reservedWord);
    }

    @Test
    public void whenRemovingAttributesFromAType_EnsureTheTypeNoLongerHasThoseAttributes(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Integer> age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
        EntityType person = tx.putEntityType("person").attribute(name).attribute(age);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age));
        person.deleteAttribute(name);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(age));
    }

    @Test
    public void whenRemovingKeysFromAType_EnsureKeysAreRemovedButAttributesAreNot(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Integer> age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
        AttributeType<Integer> id = tx.putAttributeType("id", AttributeType.DataType.INTEGER);
        EntityType person = tx.putEntityType("person").attribute(name).attribute(age).key(id);

        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age, id));
        assertThat(person.keys().collect(toSet()), containsInAnyOrder(id));

        //Nothing changes
        person.deleteAttribute(id);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age, id));
        assertThat(person.keys().collect(toSet()), containsInAnyOrder(id));

        //Key is removed
        person.deleteKey(id);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age));
        assertThat(person.keys().collect(toSet()), empty());
    }

    @Test
    public void whenCreatingAnEntityTypeWithLabelStartingWithReservedCharachter_Throw(){
        String label = "@what-a-dumb-label-name";

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidLabelStart(Label.of(label)).getMessage());

        tx.putEntityType(label);
    }

}
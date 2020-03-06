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
 *
 */

package grakn.core.concept;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.PropertyNotUniqueException;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.rule.GraknTestServer;
import grakn.core.util.ConceptDowncasting;
import graql.lang.Graql;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.CANNOT_BE_KEY_AND_ATTRIBUTE;
import static grakn.core.common.exception.ErrorMessage.UNIQUE_PROPERTY_TAKEN;
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
public class EntityTypeIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
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

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenCreatingEntityTypeUsingLabelTakenByAnotherType_Throw(){
        Role original = tx.putRole("Role Type");
        expectedException.expect(PropertyNotUniqueException.class);
        expectedException.expectMessage(PropertyNotUniqueException.cannotCreateProperty(original, Schema.VertexProperty.SCHEMA_LABEL, original.label()).getMessage());
        tx.putEntityType(original.label());
    }

    @Test
    public void whenDeletingEntityTypeWithSubTypes_Throw() throws TransactionException {
        EntityType c1 = tx.putEntityType("C1");
        EntityType c2 = tx.putEntityType("C2");
        c1.sup(c2);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(c2).getMessage());

        c2.delete();
    }

    @Test
    public void whenGettingTheLabelOfType_TheTypeLabelIsReturned(){
        Type test = tx.putEntityType("test");
        assertEquals(Label.of("test"), test.label());
    }

    @Test
    public void whenGettingTheRolesPlayedByType_ReturnTheRoles() throws Exception{
        Role monster = tx.putRole("monster");
        Role animal = tx.putRole("animal");
        Role monsterEvil = tx.putRole("evil monster").sup(monster);

        EntityType creature = tx.putEntityType("creature").plays(monster).plays(animal);
        EntityType creatureMysterious = tx.putEntityType("mysterious creature").sup(creature).plays(monsterEvil);

        assertThat(creature.playing().collect(toSet()), containsInAnyOrder(monster, animal));
        assertThat(creatureMysterious.playing().collect(toSet()), containsInAnyOrder(monster, animal, monsterEvil));
    }

    @Test
    public void whenGettingTheSuperSet_ReturnAllOfItsSuperTypes() throws Exception{
        EntityType entityType = tx.getMetaEntityType();
        EntityType c1 = tx.putEntityType("c1");
        EntityType c2 = tx.putEntityType("c2").sup(c1);
        EntityType c3 = tx.putEntityType("c3").sup(c2);
        EntityType c4 = tx.putEntityType("c4").sup(c1);

        Set<Type> c1SuperTypes = c1.sups().collect(toSet());
        Set<Type> c2SuperTypes = c2.sups().collect(toSet());
        Set<Type> c3SuperTypes = c3.sups().collect(toSet());
        Set<Type> c4SuperTypes = c4.sups().collect(toSet());

        assertThat(c1SuperTypes, containsInAnyOrder(entityType, c1));
        assertThat(c2SuperTypes, containsInAnyOrder(entityType, c2, c1));
        assertThat(c3SuperTypes, containsInAnyOrder(entityType, c3, c2, c1));
        assertThat(c4SuperTypes, containsInAnyOrder(entityType, c4, c1));
    }

    @Test
    public void whenGettingTheSuperSetViaSupsMethod_ReturnAllOfItsSuperTypes(){
        EntityType p4 = tx.putEntityType("p4");
        EntityType p3 = tx.putEntityType("p3").sup(p4);
        EntityType p2 = tx.putEntityType("p2").sup(p3);
        EntityType child = tx.putEntityType("child").sup(p2);
        EntityType entity = tx.getMetaEntityType();
        Type thing = tx.getMetaConcept();

        assertThat(child.sups().collect(toSet()), containsInAnyOrder(child, p2, p3, p4, entity));
        assertThat(p2.sups().collect(toSet()), containsInAnyOrder(p2,p3, p4, entity));
        assertThat(p3.sups().collect(toSet()), containsInAnyOrder(p3,p4, entity));
        assertThat(p4.sups().collect(toSet()), containsInAnyOrder(p4, entity));
        assertThat(entity.sups().collect(toSet()), containsInAnyOrder(entity));
        assertThat(thing.sups().collect(toSet()), empty());

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

        assertEquals(tx.getMetaEntityType(), c1.sup());
        assertEquals(c1, c2.sup());
        assertEquals(c2, c3.sup());
    }

    @Test
    public void overwriteDefaultSuperTypeWithNewSuperType_ReturnNewSuperType(){
        EntityType conceptType = tx.putEntityType("A Thing");
        EntityType conceptType2 = tx.putEntityType("A Super Thing");

        assertEquals(tx.getMetaEntityType(), conceptType.sup());
        conceptType.sup(conceptType2);
        assertEquals(conceptType2, conceptType.sup());
    }

    @Test
    public void whenRemovingRoleFromEntityType_TheRoleCanNoLongerBePlayed(){
        Role role1 = tx.putRole("A Role 1");
        Role role2 = tx.putRole("A Role 2");
        EntityType type = tx.putEntityType("A Concept Type").plays(role1).plays(role2);

        assertThat(type.playing().collect(toSet()), containsInAnyOrder(role1, role2));
        type.unplay(role1);
        assertThat(type.playing().collect(toSet()), containsInAnyOrder( role2));
    }

    @Test
    public void whenGettingTheInstancesOfType_ReturnAllInstances(){
        EntityType e1 = tx.putEntityType("e1");
        EntityType e2 = tx.putEntityType("e2").sup(e1);
        EntityType e3 = tx.putEntityType("e3").sup(e1);

        Entity e2_child1 = e2.create();
        Entity e2_child2 = e2.create();

        Entity e3_child1 = e3.create();
        Entity e3_child2 = e3.create();
        Entity e3_child3 = e3.create();

        assertThat(e1.instances().collect(toSet()), containsInAnyOrder(e2_child1, e2_child2, e3_child1, e3_child2, e3_child3));
        assertThat(e2.instances().collect(toSet()), containsInAnyOrder(e2_child1, e2_child2));
        assertThat(e3.instances().collect(toSet()), containsInAnyOrder(e3_child1, e3_child2, e3_child3));
    }

    @Test
    public void settingTheSuperTypeToItself_Throw(){
        EntityType entityType = tx.putEntityType("Entity");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.loopCreated(entityType, entityType).getMessage());
        entityType.sup(entityType);
    }

    @Test
    public void whenCyclicSuperTypes_Throw(){
        EntityType entityType1 = tx.putEntityType("Entity1");
        EntityType entityType2 = tx.putEntityType("Entity2");
        EntityType entityType3 = tx.putEntityType("Entity3");
        entityType1.sup(entityType2);
        entityType2.sup(entityType3);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.loopCreated(entityType3, entityType1).getMessage());

        entityType3.sup(entityType1);
    }

    @Test
    public void whenSettingMetaTypeToAbstract_Throw(){
        Type meta = tx.getMetaEntityType();

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.metaTypeImmutable(meta.label()).getMessage());

        meta.isAbstract(true);
    }

    @Test
    public void whenAddingRoleToMetaType_Throw(){
        Type meta = tx.getMetaEntityType();
        Role role = tx.putRole("A Role");

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.metaTypeImmutable(meta.label()).getMessage());

        meta.plays(role);
    }

    @Test
    public void whenAddingResourcesWithSubTypesToEntityTypes_EnsureImplicitStructureFollowsSubTypes(){
        EntityType entityType1 = tx.putEntityType("Entity Type 1");
        EntityType entityType2 = tx.putEntityType("Entity Type 2");

        Label superLabel = Label.of("Super Attribute Type");
        Label label = Label.of("Attribute Type");

        AttributeType superAttributeType = tx.putAttributeType(superLabel, AttributeType.DataType.STRING);
        AttributeType attributeType = tx.putAttributeType(label, AttributeType.DataType.STRING).sup(superAttributeType);
        AttributeType metaType = tx.getMetaAttributeType();

        entityType1.has(superAttributeType);
        entityType2.has(attributeType);

        //Check role types are only built explicitly
        assertThat(entityType1.playing().collect(toSet()),
                containsInAnyOrder(tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(superLabel).getValue())));

        assertThat(entityType2.playing().collect(toSet()),
                containsInAnyOrder(tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(label).getValue())));

        //Check Implicit Types Follow SUB Structure
        RelationType superRelation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(superAttributeType.label()).getValue());
        Role superRoleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(superAttributeType.label()).getValue());
        Role superRoleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(superAttributeType.label()).getValue());

        RelationType relation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(attributeType.label()).getValue());
        Role roleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(attributeType.label()).getValue());
        Role roleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(attributeType.label()).getValue());

        RelationType metaRelation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(metaType.label()).getValue());
        Role metaRoleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(metaType.label()).getValue());
        Role metaRoleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(metaType.label()).getValue());

        assertEquals(superRoleOwner, roleOwner.sup());
        assertEquals(superRoleValue, roleValue.sup());
        assertEquals(superRelation, relation.sup());

        assertEquals(metaRoleOwner, superRoleOwner.sup());
        assertEquals(metaRoleValue, superRoleValue.sup());
        assertEquals(metaRelation, superRelation.sup());
    }

    @Test
    public void whenAddingResourceWithAbstractSuperTypeToEntityType_EnsureImplicitStructureFollowsSubTypes(){
        EntityType entityType = tx.putEntityType("Entity Type");

        Label superLabel = Label.of("Abstract Super Attribute Type");
        Label label = Label.of("Attribute Type");

        AttributeType superAttributeType = tx.putAttributeType(superLabel, AttributeType.DataType.STRING);
        AttributeType attributeType = tx.putAttributeType(label, AttributeType.DataType.STRING).sup(superAttributeType);
        AttributeType metaType = tx.getMetaAttributeType();

        entityType.has(attributeType);

        //Check role types are only built explicitly
        assertThat(entityType.playing().collect(toSet()),
                containsInAnyOrder(tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(label).getValue())));

        //Check Implicit Types Follow SUB Structure
        RelationType superRelation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(superAttributeType.label()).getValue());
        Role superRoleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(superAttributeType.label()).getValue());
        Role superRoleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(superAttributeType.label()).getValue());

        RelationType relation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(attributeType.label()).getValue());
        Role roleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(attributeType.label()).getValue());
        Role roleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(attributeType.label()).getValue());

        RelationType metaRelation = tx.getRelationType(Schema.ImplicitType.HAS.getLabel(metaType.label()).getValue());
        Role metaRoleOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(metaType.label()).getValue());
        Role metaRoleValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(metaType.label()).getValue());

        assertEquals(superRoleOwner, roleOwner.sup());
        assertEquals(superRoleValue, roleValue.sup());
        assertEquals(superRelation, relation.sup());

        assertEquals(metaRoleOwner, superRoleOwner.sup());
        assertEquals(metaRoleValue, superRoleValue.sup());
        assertEquals(metaRelation, superRelation.sup());
    }

    @Test
    public void whenDeletingTypeWithEntities_Throw(){
        EntityType entityTypeA = tx.putEntityType("entityTypeA");
        EntityType entityTypeB = tx.putEntityType("entityTypeB");

        entityTypeB.create();

        entityTypeA.delete();
        assertNull(tx.getEntityType("entityTypeA"));

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(entityTypeB).getMessage());

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
        e1.has(r1);
        e1.has(r2);
        e1.has(r3);
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
        entityType1.has(attributeType1);

        entityType1.key(attributeType2);
        entityType2.key(attributeType1);
        entityType2.key(attributeType2);

        assertThat(entityType1.attributes().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));
        assertThat(entityType2.attributes().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));

        assertThat(entityType1.keys().collect(toSet()), containsInAnyOrder(attributeType2));
        assertThat(entityType2.keys().collect(toSet()), containsInAnyOrder(attributeType1, attributeType2));

        //Add resource which is a key for one entity and a resource for another
        Entity entity1 = entityType1.create();
        Entity entity2 = entityType2.create();
        Attribute<String> attribute1 = attributeType1.create("Test 1");
        Attribute<String> attribute2 = attributeType2.create("Test 2");
        Attribute<String> attribute3 = attributeType2.create("Test 3");

        //Attribute 1 is a key to one and a resource to another
        entity1.has(attribute1);
        entity2.has(attribute1);

        entity1.has(attribute2);
        entity2.has(attribute3);

        tx.commit();
    }

    @Test
    public void whenAddingResourceTypeAsKeyAfterResource_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("Shared Attribute", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("EntityType");

        entityType.has(attributeType);

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_ATTRIBUTE.getMessage(entityType.label(), attributeType.label()));

        entityType.key(attributeType);
    }

    @Test
    public void whenAddingResourceTypeAsResourceAfterResource_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("Shared Attribute", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("EntityType");

        entityType.key(attributeType);

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(CANNOT_BE_KEY_AND_ATTRIBUTE.getMessage(entityType.label(), attributeType.label()));

        entityType.has(attributeType);
    }

    @Test
    public void whenCreatingEntityType_EnsureItHasAShard(){
        EntityType entityType = tx.putEntityType("EntityType");
        assertThat(ConceptDowncasting.type(entityType).shards().collect(toSet()), not(empty()));
        assertEquals(ConceptDowncasting.type(entityType).shards().iterator().next(), ConceptDowncasting.type(entityType).currentShard());
    }

    @Test
    public void whenAddingInstanceToType_EnsureIsaEdgeIsPlacedOnShard(){
        EntityType entityType = tx.putEntityType("EntityType");
        Shard shard =  ConceptDowncasting.type(entityType).currentShard();
        Entity e1 = entityType.create();

        assertFalse("The isa edge was places on the type rather than the shard", ConceptDowncasting.type(entityType)
                .neighbours(Direction.IN, Schema.EdgeLabel.ISA).iterator().hasNext());
        assertEquals(e1, tx.getConcept(Schema.conceptId(shard.links().findAny().get().element())));
    }

    @Test
    public void whenAddingTypeUsingReservedWord_ThrowReadableError(){
        Concept thing = tx.getMetaConcept();
        String reservedWord = Graql.Token.Type.THING.toString();

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(UNIQUE_PROPERTY_TAKEN.getMessage(Schema.VertexProperty.SCHEMA_LABEL, "thing", thing));

        tx.putEntityType(reservedWord);
    }

    @Test
    public void whenRemovingAttributesFromAType_EnsureTheTypeNoLongerHasThoseAttributes(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Integer> age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
        EntityType person = tx.putEntityType("person").has(name).has(age);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age));
        person.unhas(name);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(age));
    }

    @Test
    public void whenRemovingKeysFromAType_EnsureKeysAreRemovedt(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Integer> age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
        AttributeType<Integer> id = tx.putAttributeType("id", AttributeType.DataType.INTEGER);
        EntityType person = tx.putEntityType("person").has(name).has(age).key(id);

        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age, id));
        assertThat(person.keys().collect(toSet()), containsInAnyOrder(id));

        //Key is removed
        person.unkey(id);
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age));
        assertThat(person.keys().collect(toSet()), empty());
    }

    @Test
    public void whenRemovingKeyAsHas_Throw(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<Integer> age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
        AttributeType<Integer> id = tx.putAttributeType("id", AttributeType.DataType.INTEGER);
        EntityType person = tx.putEntityType("person").has(name).has(age).key(id);

        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(name, age, id));
        assertThat(person.keys().collect(toSet()), containsInAnyOrder(id));

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.illegalUnhasNotExist(
                person.label().getValue(), id.label().getValue(), false).getMessage());
        person.unhas(id);
    }

    @Test
    public void whenCreatingAnEntityTypeWithLabelStartingWithReservedCharachter_Throw(){
        String label = "@what-a-dumb-label-name";

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.invalidLabelStart(Label.of(label)).getMessage());

        tx.putEntityType(label);
    }

}
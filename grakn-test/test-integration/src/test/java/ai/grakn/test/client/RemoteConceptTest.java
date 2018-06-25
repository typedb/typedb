/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.client;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.client.concept.RemoteAttribute;
import ai.grakn.client.concept.RemoteAttributeType;
import ai.grakn.client.concept.RemoteEntity;
import ai.grakn.client.concept.RemoteEntityType;
import ai.grakn.client.concept.RemoteRelationship;
import ai.grakn.client.concept.RemoteRole;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.AttributeType.DataType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit Test for testing methods for all subclasses of {@link ai.grakn.client.concept.RemoteConcept}.
 */
public class RemoteConceptTest {

    private static final ConceptId A = ConceptId.of("A");
    private static final ConceptId B = ConceptId.of("B");
    private static final ConceptId C = ConceptId.of("C");
//
//    @Rule
//    public final ServerRPCMock server = ServerRPCMock.create();
//
//    private Grakn.Session session;
//    private Grakn.Transaction tx;
//    private static final SimpleURI URI = new SimpleURI("localhost", 999);
//    private static final Label LABEL = Label.of("too-tired-for-funny-test-names-today");

    SchemaConcept schemaConcept;
    private Type type;
    private EntityType entityType;
    private AttributeType<String> attributeType;
    private RelationshipType relationshipType;
    private Role role;
    private ai.grakn.concept.Rule rule;
    private Entity entity;
    private Attribute<String> attribute;
    private Relationship relationship;
    private Thing thing;
    private Concept concept;

    @ClassRule
    public static final EngineContext engine = EngineContext.create();
    private Grakn.Session session;
    private Grakn.Transaction tx;




    // Attribute Type Labels
    private Label EMAIL = Label.of("email");
    private Label NAME = Label.of("name");
    private Label AGE = Label.of("age");

    private String EMAIL_REGEX = "\\S+@\\S+";

    // Entity Type Labels
    private Label LIVING_THING = Label.of("living-thing");
    private Label PERSON = Label.of("person");

    // Relationship Type Labels
    private Label HUSBAND = Label.of("husband");
    private Label WIFE = Label.of("wife");
    private Label MARRIAGE = Label.of("marriage");

    // Attribute values
    private String ALICE = "Alice";
    private String ALICE_EMAIL = "alice@email.com";
    private String BOB = "Bob";
    private String BOB_EMAIL = "bob@email.com";
    private Integer TWENTY = 20;

    private AttributeType<Integer> age;
    private AttributeType<String> name;
    private AttributeType<String> email;
    private EntityType livingThing;
    private EntityType person;
    private Role husband;
    private Role wife;
    private RelationshipType marriage;

    private Attribute<String> emailAlice;
    private Attribute<String> emailBob;
    private Attribute<Integer> age20;
    private Attribute<String> nameAlice;
    private Attribute<String> nameBob;
    private Entity alice;
    private Entity bob;
    private Relationship aliceAndBob;

    @Before
    public void setUp() {
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        session = Grakn.session(engine.grpcUri(), keyspace);
        tx = session.transaction(GraknTxType.WRITE);

        // Attribute Types
        email = tx.putAttributeType(EMAIL, DataType.STRING).setRegex(EMAIL_REGEX);
        name = tx.putAttributeType(NAME, DataType.STRING);
        age = tx.putAttributeType(AGE, DataType.INTEGER);

        // Entity Types
        livingThing = tx.putEntityType(LIVING_THING).setAbstract(true);
        person = tx.putEntityType(PERSON);
        person.sup(livingThing);
        person.key(email);
        person.attribute(name);
        person.attribute(age);

        // Relationship Types
        husband = tx.putRole(HUSBAND);
        wife = tx.putRole(WIFE);
        marriage = tx.putRelationshipType(MARRIAGE).relates(wife).relates(husband);
        person.plays(wife).plays(husband);

        // Attributes
        emailAlice = email.putAttribute(ALICE_EMAIL);
        emailBob = email.putAttribute(BOB_EMAIL);
        nameAlice = name.putAttribute(ALICE);
        nameBob = name.putAttribute(BOB);
        age20 = age.putAttribute(TWENTY);

        // Entities
        alice = person.addEntity().attribute(emailAlice).attribute(nameAlice).attribute(age20);
        bob = person.addEntity().attribute(emailBob).attribute(nameBob).attribute(age20);

        // Relationships
        aliceAndBob = marriage.addRelationship().addRolePlayer(wife, alice).addRolePlayer(husband, bob);

//        entityType = RemoteEntityType.create(tx, ID);
//        attributeType = RemoteAttributeType.create(tx, ID);
//        relationshipType = RemoteRelationshipType.create(tx, ID);
//        role = RemoteRole.create(tx, ID);
//        rule = RemoteRule.create(tx, ID);
//        schemaConcept = role;
//        type = entityType;
//
//        entity = RemoteEntity.create(tx, ID);
//        attribute = RemoteAttribute.create(tx, ID);
//        relationship = RemoteRelationship.create(tx, ID);
//        thing = entity;
//        concept = entity;
    }

    @After
    public void closeTx() {
        tx.close();
    }

    @After
    public void closeSession() {
        session.close();
    }

    @Test
    public void whenGettingLabel_ReturnTheExpectedLabel() {
        assertEquals(EMAIL, email.getLabel());
        assertEquals(NAME, name.getLabel());
        assertEquals(AGE, age.getLabel());
        assertEquals(PERSON, person.getLabel());
        assertEquals(HUSBAND, husband.getLabel());
        assertEquals(WIFE, wife.getLabel());
        assertEquals(MARRIAGE, marriage.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        email.plays().forEach(role -> assertTrue(role.isImplicit()));
        name.plays().forEach(role -> assertTrue(role.isImplicit()));
        age.plays().forEach(role -> assertTrue(role.isImplicit()));
    }

    @Test
    public void whenCallingIsAbstract_GetTheExpectedResult() {
        assertTrue(livingThing.isAbstract());
    }

    @Test
    public void whenCallingGetValue_GetTheExpectedResult() {
        assertEquals(ALICE_EMAIL, emailAlice.getValue());
        assertEquals(BOB_EMAIL, emailBob.getValue());
        assertEquals(ALICE, nameAlice.getValue());
        assertEquals(BOB, nameBob.getValue());
        assertEquals(TWENTY, age20.getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        assertEquals(DataType.STRING, email.getDataType());
        assertEquals(DataType.STRING, name.getDataType());
        assertEquals(DataType.INTEGER, age.getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        assertEquals(DataType.STRING, emailAlice.dataType());
        assertEquals(DataType.STRING, emailBob.dataType());
        assertEquals(DataType.STRING, nameAlice.dataType());
        assertEquals(DataType.STRING, nameBob.dataType());
        assertEquals(DataType.INTEGER, age20.dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        assertEquals(EMAIL_REGEX, email.getRegex());
    }

    @Test
    public void whenCallingGetAttribute_GetTheExpectedResult() {
        assertEquals(emailAlice, email.getAttribute(ALICE_EMAIL));
        assertEquals(emailBob, email.getAttribute(BOB_EMAIL));
        assertEquals(nameAlice, name.getAttribute(ALICE));
        assertEquals(nameBob, name.getAttribute(BOB));
        assertEquals(age20, age.getAttribute(TWENTY));
    }

    @Test
    public void whenCallingGetAttributeWhenThereIsNoResult_ReturnNull() {
        assertNull(email.getAttribute("x@x.com"));
        assertNull(name.getAttribute("random"));
        assertNull(age.getAttribute(-1));
    }

    @Test @Ignore //TODO: build a more expressive dataset to test this
    public void whenCallingIsInferred_GetTheExpectedResult() {
        //mockConceptMethod(isInferred, true);
        assertTrue(thing.isInferred());

        //mockConceptMethod(isInferred, false);
        assertFalse(thing.isInferred());
    }

    @Test @Ignore //TODO: build a more expressive dataset to test this
    public void whenCallingGetWhen_GetTheExpectedResult() {
        //assertEquals(PATTERN, rule.getWhen());
    }

    @Test @Ignore //TODO: build a more expressive dataset to test this
    public void whenCallingGetThen_GetTheExpectedResult() {
        //assertEquals(PATTERN, rule.getThen());
    }

    @Test
    public void whenCallingIsDeleted_GetTheExpectedResult() {
        Entity randomPerson = person.addEntity();
        assertFalse(randomPerson.isDeleted());

        randomPerson.delete();
        assertTrue(randomPerson.isDeleted());
    }

    @Test
    public void whenCallingSups_GetTheExpectedResult() {
        assertTrue(person.sups().collect(toSet()).contains(livingThing));
    }

    @Test
    public void whenCallingSubs_GetTheExpectedResult() {
        assertTrue(livingThing.subs().collect(toSet()).contains(person));
    }

    @Test
    public void whenCallingSup_GetTheExpectedResult() {
        assertEquals(livingThing, person.sup());
    }

    @Test
    public void whenCallingSupOnMetaType_GetNull() {
        assertNull(tx.getEntityType("entity").sup());
    }

    @Test
    public void whenCallingType_GetTheExpectedResult() {
        assertEquals(email, emailAlice.type());
        assertEquals(email, emailBob.type());
        assertEquals(name, nameAlice.type());
        assertEquals(name, nameBob.type());
        assertEquals(age, age20.type());
        assertEquals(person, alice.type());
        assertEquals(person, bob.type());
        assertEquals(marriage, aliceAndBob.type());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_GetTheExpectedResult() {
        assertThat(alice.attributes().collect(toSet()), containsInAnyOrder(emailAlice, nameAlice, age20));
        assertThat(bob.attributes().collect(toSet()), containsInAnyOrder(emailBob, nameBob, age20));
    }

    @Test
    public void whenCallingAttributesWithArguments_GetTheExpectedResult() {
        assertThat(alice.attributes(email, age).collect(toSet()), containsInAnyOrder(emailAlice, age20));
        assertThat(bob.attributes(email, age).collect(toSet()), containsInAnyOrder(emailBob, age20));
    }

    @Test
    public void whenCallingKeysWithNoArguments_GetTheExpectedResult() {
        assertThat(alice.keys().collect(toSet()), contains(emailAlice));
        assertThat(bob.keys().collect(toSet()), contains(emailBob));
    }

    @Test
    public void whenCallingKeysWithArguments_GetTheExpectedResult() {
        assertThat(alice.keys(email).collect(toSet()), contains(emailAlice));
        assertThat(bob.keys(email).collect(toSet()), contains(emailBob));
    }

    @Test
    public void whenCallingPlays_GetTheExpectedResult() {
        assertThat(person.plays().filter(r -> !r.isImplicit()).collect(toSet()), containsInAnyOrder(wife, husband));
    }

    @Test
    public void whenCallingInstances_GetTheExpectedResult() {
        assertThat(email.instances().collect(toSet()), containsInAnyOrder(emailAlice, emailBob));
        assertThat(name.instances().collect(toSet()), containsInAnyOrder(nameAlice, nameBob));
        assertThat(age.instances().collect(toSet()), containsInAnyOrder(age20));
        assertThat(person.instances().collect(toSet()), containsInAnyOrder(alice, bob));
        assertThat(marriage.instances().collect(toSet()), containsInAnyOrder(aliceAndBob));
    }

    @Test
    public void whenCallingThingPlays_GetTheExpectedResult() {
        assertThat(alice.plays().filter(r -> !r.isImplicit()).collect(toSet()), containsInAnyOrder(wife));
        assertThat(bob.plays().filter(r -> !r.isImplicit()).collect(toSet()), containsInAnyOrder(husband));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_GetTheExpectedResult() {
        assertThat(alice.relationships().filter(rel -> !rel.type().isImplicit()).collect(toSet()), containsInAnyOrder(aliceAndBob));
        assertThat(bob.relationships().filter(rel -> !rel.type().isImplicit()).collect(toSet()), containsInAnyOrder(aliceAndBob));
    }

    @Test
    public void whenCallingRelationshipsWithRoles_GetTheExpectedResult() {
        assertThat(alice.relationships(wife).collect(toSet()), containsInAnyOrder(aliceAndBob));
        assertThat(bob.relationships(husband).collect(toSet()), containsInAnyOrder(aliceAndBob));
    }

    @Test
    public void whenCallingRelationshipTypes_GetTheExpectedResult() {
        assertThat(wife.relationshipTypes().collect(toSet()), containsInAnyOrder(marriage));
        assertThat(husband.relationshipTypes().collect(toSet()), containsInAnyOrder(marriage));
    }

    @Test
    public void whenCallingPlayedByTypes_GetTheExpectedResult() {
        assertThat(wife.playedByTypes().collect(toSet()), containsInAnyOrder(person));
        assertThat(husband.playedByTypes().collect(toSet()), containsInAnyOrder(person));
    }

    @Test
    public void whenCallingRelates_GetTheExpectedResult() {
        assertThat(marriage.relates().collect(toSet()), containsInAnyOrder(wife, husband));
    }

    @Test
    public void whenCallingAllRolePlayers_GetTheExpectedResult() {
        Map<Role, Set<Thing>> expected = ImmutableMap.of(
                wife, ImmutableSet.of(alice),
                husband, ImmutableSet.of(bob)
        );
        assertEquals(expected, aliceAndBob.allRolePlayers());
    }

    @Test
    public void whenCallingRolePlayersWithNoArguments_GetTheExpectedResult() {
        assertThat(aliceAndBob.rolePlayers().collect(toSet()), containsInAnyOrder(alice, bob));
    }

    @Test
    public void whenCallingRolePlayersWithRoles_GetTheExpectedResult() {
        assertThat(aliceAndBob.rolePlayers(wife).collect(toSet()), containsInAnyOrder(alice));
        assertThat(aliceAndBob.rolePlayers(husband).collect(toSet()), containsInAnyOrder(bob));
    }

    @Test
    public void whenCallingOwnerInstances_GetTheExpectedResult() {
        assertThat(emailAlice.ownerInstances().collect(toSet()), containsInAnyOrder(alice));
        assertThat(emailBob.ownerInstances().collect(toSet()), containsInAnyOrder(bob));
        assertThat(nameAlice.ownerInstances().collect(toSet()), containsInAnyOrder(alice));
        assertThat(nameBob.ownerInstances().collect(toSet()), containsInAnyOrder(bob));
        assertThat(age20.ownerInstances().collect(toSet()), containsInAnyOrder(alice, bob));
    }

    @Test @Ignore
    public void whenCallingAttributeTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> attributeTypes = ImmutableSet.of(
                RemoteAttributeType.create(tx, A),
                RemoteAttributeType.create(tx, B),
                RemoteAttributeType.create(tx, C)
        );

        //mockConceptMethod(getAttributeTypes, attributeTypes.stream());

        assertEquals(attributeTypes, type.attributes().collect(toSet()));
    }

    @Test @Ignore
    public void whenCallingKeyTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> keyTypes = ImmutableSet.of(
                RemoteAttributeType.create(tx, A),
                RemoteAttributeType.create(tx, B),
                RemoteAttributeType.create(tx, C)
        );

        //mockConceptMethod(getKeyTypes, keyTypes.stream());

        assertEquals(keyTypes, type.keys().collect(toSet()));
    }

    @Test @Ignore
    public void whenCallingDelete_ExecuteAConceptMethod() {
        concept.delete();
        //verifyConceptMethodCalled(ConceptMethod.delete);
    }

    @Test @Ignore
    public void whenSettingSuper_ExecuteAConceptMethod() {
        EntityType sup = RemoteEntityType.create(tx, A);
        assertEquals(entityType, entityType.sup(sup));
        //verifyConceptMethodCalled(ConceptMethod.setDirectSuper(sup));
    }

    @Test @Ignore
    public void whenSettingSub_ExecuteAConceptMethod() {
        EntityType sup = RemoteEntityType.create(tx, A);
        assertEquals(sup, sup.sub(entityType));
        //verifyConceptMethodCalled(ConceptMethod.setDirectSuper(sup));
    }

    @Test @Ignore
    public void whenSettingLabel_ExecuteAConceptMethod() {
        Label label = Label.of("Dunstan");
        assertEquals(schemaConcept, schemaConcept.setLabel(label));
        //verifyConceptMethodCalled(ConceptMethod.setLabel(label));
    }

    @Test @Ignore
    public void whenSettingRelates_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        assertEquals(relationshipType, relationshipType.relates(role));
        //verifyConceptMethodCalled(ConceptMethod.setRelatedRole(role));
    }

    @Test @Ignore
    public void whenSettingPlays_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        assertEquals(attributeType, attributeType.plays(role));
        //verifyConceptMethodCalled(ConceptMethod.setRolePlayedByType(role));
    }

    @Test @Ignore
    public void whenSettingAbstractOn_ExecuteAConceptMethod() {
        assertEquals(attributeType, attributeType.setAbstract(true));
        //verifyConceptMethodCalled(ConceptMethod.setAbstract(true));
    }

    @Test @Ignore
    public void whenSettingAbstractOff_ExecuteAConceptMethod() {
        assertEquals(attributeType, attributeType.setAbstract(false));
        //verifyConceptMethodCalled(ConceptMethod.setAbstract(false));
    }

    @Test @Ignore
    public void whenSettingAttributeType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteAttributeType.create(tx, A);
        assertEquals(type, type.attribute(attributeType));
        //verifyConceptMethodCalled(ConceptMethod.setAttributeType(attributeType));
    }

    @Test @Ignore
    public void whenSettingKeyType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteAttributeType.create(tx, A);
        assertEquals(type, type.key(attributeType));
        //verifyConceptMethodCalled(ConceptMethod.setKeyType(attributeType));
    }

    @Test @Ignore
    public void whenDeletingAttributeType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteAttributeType.create(tx, A);
        assertEquals(type, type.deleteAttribute(attributeType));
        //verifyConceptMethodCalled(ConceptMethod.unsetAttributeType(attributeType));
    }

    @Test @Ignore
    public void whenDeletingKeyType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteAttributeType.create(tx, A);
        assertEquals(type, type.deleteKey(attributeType));
        //verifyConceptMethodCalled(ConceptMethod.unsetKeyType(attributeType));
    }

    @Test @Ignore
    public void whenDeletingPlays_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        assertEquals(type, type.deletePlays(role));
        //verifyConceptMethodCalled(ConceptMethod.unsetRolePlayedByType(role));
    }

    @Test @Ignore
    public void whenCallingAddEntity_ExecuteAConceptMethod() {
        Entity entity = RemoteEntity.create(tx, A);
        //mockConceptMethod(ConceptMethod.addEntity, entity);
        assertEquals(entity, entityType.addEntity());
    }

    @Test @Ignore
    public void whenCallingAddRelationship_ExecuteAConceptMethod() {
        Relationship relationship = RemoteRelationship.create(tx, A);
        //mockConceptMethod(ConceptMethod.addRelationship, relationship);
        assertEquals(relationship, relationshipType.addRelationship());
    }

    @Test @Ignore
    public void whenCallingPutAttribute_ExecuteAConceptMethod() {
        String value = "Dunstan";
        Attribute<String> attribute = RemoteAttribute.create(tx, A);
        //mockConceptMethod(ConceptMethod.putAttribute(value), attribute);
        assertEquals(attribute, attributeType.putAttribute(value));
    }

    @Test @Ignore
    public void whenCallingDeleteRelates_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        assertEquals(relationshipType, relationshipType.deleteRelates(role));
        //verifyConceptMethodCalled(ConceptMethod.unsetRelatedRole(role));
    }

    @Test @Ignore
    public void whenSettingRegex_ExecuteAConceptMethod() {
        String regex = "[abc]";
        assertEquals(attributeType, attributeType.setRegex(regex));
        //verifyConceptMethodCalled(ConceptMethod.setRegex(Optional.of(regex)));
    }

    @Test @Ignore
    public void whenResettingRegex_ExecuteAQuery() {
        assertEquals(attributeType, attributeType.setRegex(null));
        //verifyConceptMethodCalled(ConceptMethod.setRegex(Optional.empty()));
    }

    @Test @Ignore
    public void whenCallingAddAttributeOnThing_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteAttribute.create(tx, A);
        Relationship relationship = RemoteRelationship.create(tx, C);
        //mockConceptMethod(ConceptMethod.setAttribute(attribute), relationship);

        assertEquals(thing, thing.attribute(attribute));

        //verifyConceptMethodCalled(ConceptMethod.setAttribute(attribute));
    }

    @Test @Ignore
    public void whenCallingAddAttributeRelationshipOnThing_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteAttribute.create(tx, A);
        Relationship relationship = RemoteRelationship.create(tx, C);
        //mockConceptMethod(ConceptMethod.setAttribute(attribute), relationship);
        assertEquals(relationship, thing.attributeRelationship(attribute));
    }

    @Test @Ignore
    public void whenCallingDeleteAttribute_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteAttribute.create(tx, A);
        assertEquals(thing, thing.deleteAttribute(attribute));
        //verifyConceptMethodCalled(ConceptMethod.unsetAttribute(attribute));
    }

    @Test @Ignore
    public void whenCallingAddRolePlayer_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        Thing thing = RemoteEntity.create(tx, B);
        assertEquals(relationship, relationship.addRolePlayer(role, thing));

        //verifyConceptMethodCalled(ConceptMethod.setRolePlayer(RolePlayer.create(role, thing)));
    }

    @Test @Ignore
    public void whenCallingRemoveRolePlayer_ExecuteAConceptMethod() {
        Role role = RemoteRole.create(tx, A);
        Thing thing = RemoteEntity.create(tx, B);
        relationship.removeRolePlayer(role, thing);
        //verifyConceptMethodCalled(ConceptMethod.removeRolePlayer(RolePlayer.create(role, thing)));
    }

//    private void verifyConceptMethodCalled(ConceptMethod<?> conceptMethod) {
//        verify(server.requests()).onNext(RequestBuilder.runConceptMethod(ID, conceptMethod));
//    }

//    private <T> void mockConceptMethod(ConceptMethod<T> property, T value) {
//        server.setResponse(
//                RequestBuilder.runConceptMethod(ID, property),
//                property.createTxResponse(server.grpcIterators(), value)
//        );
//    }
}
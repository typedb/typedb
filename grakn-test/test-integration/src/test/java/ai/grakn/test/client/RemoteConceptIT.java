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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.AttributeType.DataType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
 * Integration Test for testing methods for all subclasses of {@link ai.grakn.client.concept.RemoteConcept}.
 */
public class RemoteConceptIT {

    @ClassRule
    public static final EngineContext engine = EngineContext.create();
    private static Grakn.Session session;
    private Grakn.Transaction tx;

    // Attribute Type Labels
    private Label EMAIL = Label.of("email");
    private Label NAME = Label.of("name");
    private Label AGE = Label.of("age");

    private String EMAIL_REGEX = "\\S+@\\S+";
    private static int EMAIL_COUNTER = 0;

    // Entity Type Labels
    private Label LIVING_THING = Label.of("living-thing");
    private Label PERSON = Label.of("person");
    private Label MAN = Label.of("man");
    private Label BOY = Label.of("boy");

    // Relationship Type Labels
    private Label HUSBAND = Label.of("husband");
    private Label WIFE = Label.of("wife");
    private Label MARRIAGE = Label.of("marriage");

    private Label FRIEND = Label.of("friend");
    private Label FRIENDSHIP = Label.of("friendship");

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
    private EntityType man;
    private EntityType boy;
    private Role husband;
    private Role wife;
    private RelationshipType marriage;
    private Role friend;
    private RelationshipType friendship;

    private Attribute<String> emailAlice;
    private Attribute<String> emailBob;
    private Attribute<Integer> age20;
    private Attribute<String> nameAlice;
    private Attribute<String> nameBob;
    private Entity alice;
    private Entity bob;
    private Relationship aliceAndBob;

    @BeforeClass
    public static void setUpClass() {
        // TODO: uncomment the code below to confirm we have fixed/removed our Core API Cache
        // session = Grakn.session(engine.grpcUri(), SampleKBLoader.randomKeyspace());
    }

    @Before
    public void setUp() {
        // move session construction to setupClass
        session = Grakn.session(engine.grpcUri(), SampleKBLoader.randomKeyspace());

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

        man = tx.putEntityType(MAN);
        boy = tx.putEntityType(BOY);

        // Relationship Types
        husband = tx.putRole(HUSBAND);
        wife = tx.putRole(WIFE);
        marriage = tx.putRelationshipType(MARRIAGE).relates(wife).relates(husband);


        friend = tx.putRole(FRIEND);
        friendship = tx.putRelationshipType(FRIENDSHIP);

        person.plays(wife).plays(husband);

        // Attributes
        EMAIL_COUNTER++;
        emailAlice = email.putAttribute(ALICE_EMAIL + EMAIL_COUNTER);
        emailBob = email.putAttribute(BOB_EMAIL + EMAIL_COUNTER);

        nameAlice = name.putAttribute(ALICE);
        nameBob = name.putAttribute(BOB);
        age20 = age.putAttribute(TWENTY);

        // Entities
        alice = person.addEntity().attribute(emailAlice).attribute(nameAlice).attribute(age20);
        bob = person.addEntity().attribute(emailBob).attribute(nameBob).attribute(age20);

        // Relationships
        aliceAndBob = marriage.addRelationship().addRolePlayer(wife, alice).addRolePlayer(husband, bob);
    }

    @After
    public void closeTx() {
        tx.close();
        session.close();
    }

    @AfterClass
    public static void closeSession() {
        // TODO: uncomment the code below to confirm we have fixed/removed our Core API Cache
        //session.close();
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
        assertEquals(ALICE_EMAIL + EMAIL_COUNTER, emailAlice.getValue());
        assertEquals(BOB_EMAIL + EMAIL_COUNTER, emailBob.getValue());
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
        assertEquals(emailAlice, email.getAttribute(ALICE_EMAIL + EMAIL_COUNTER));
        assertEquals(emailBob, email.getAttribute(BOB_EMAIL + EMAIL_COUNTER));
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

    @Ignore @Test //TODO: build a more expressive dataset to test this
    public void whenCallingIsInferred_GetTheExpectedResult() {
        //assertTrue(thing.isInferred());
        //assertFalse(thing.isInferred());
    }

    @Ignore @Test //TODO: build a more expressive dataset to test this
    public void whenCallingGetWhen_GetTheExpectedResult() {
        //assertEquals(PATTERN, rule.getWhen());
    }

    @Ignore @Test //TODO: build a more expressive dataset to test this
    public void whenCallingGetThen_GetTheExpectedResult() {
        //assertEquals(PATTERN, rule.getThen());
    }

    @Test
    public void whenDeletingAConcept_ConceptIsDeleted() {
        Entity randomPerson = person.addEntity();
        assertFalse(randomPerson.isDeleted());

        randomPerson.delete();
        assertTrue(randomPerson.isDeleted());
    }

    @Test
    public void whenCallingSups_GetTheExpectedResult() {
        assertTrue(person.sups().anyMatch(c -> c.equals(livingThing)));
    }

    @Test
    public void whenCallingSubs_GetTheExpectedResult() {
        assertTrue(livingThing.subs().anyMatch(c -> c.equals(person)));
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

    @Test
    public void whenCallingAttributeTypes_GetTheExpectedResult() {
        assertThat(person.attributes().collect(toSet()), containsInAnyOrder(email, name, age));
    }

    @Test
    public void whenCallingKeyTypes_GetTheExpectedResult() {
        assertThat(person.keys().collect(toSet()), containsInAnyOrder(email));
    }

    @Test
    public void whenSettingSuperType_TypeBecomesSupertype() {
        man.sup(person);
        assertEquals(person, man.sup());
    }

    @Test
    public void whenSettingSubType_TypeBecomesSubtype() {
        man.sub(boy);
        assertTrue(man.subs().anyMatch(c -> c.equals(boy)));
    }

    @Test
    public void whenSettingTypeLabel_LabelIsSetToType() {
        Label lady = Label.of("lady");
        EntityType type = tx.putEntityType(lady);
        assertEquals(lady, type.getLabel());

        Label woman = Label.of("woman");
        type.setLabel(woman);
        assertEquals(woman, type.getLabel());
    }

    @Test
    public void whenSettingAndDeletingRelationshipRelatesRole_RoleInRelationshipIsSetAndDeleted() {
        friendship.relates(friend);
        assertTrue(friendship.relates().anyMatch(c -> c.equals(friend)));

        friendship.deleteRelates(friend);
        assertFalse(friendship.relates().anyMatch(c -> c.equals(friend)));
    }

    @Test
    public void whenSettingAndDeletingEntityPlaysRole_RolePlaysEntityIsSetAndDeleted() {
        person.plays(friend);
        assertTrue(person.plays().anyMatch(c -> c.equals(friend)));

        person.deletePlays(friend);
        assertFalse(person.plays().anyMatch(c -> c.equals(friend)));
    }

    @Test
    public void whenSettingAndUnsettingAbstractType_TypeAbstractIsSetAndUnset() {
        livingThing.setAbstract(false);
        assertFalse(livingThing.isAbstract());

        livingThing.setAbstract(true);
        assertTrue(livingThing.isAbstract());
    }

    @Test
    public void whenSettingAndDeletingAttributeToType_AttributeIsSetAndDeleted() {
        EntityType cat = tx.putEntityType(Label.of("cat"));
        cat.attribute(name);
        assertTrue(cat.attributes().anyMatch(c -> c.equals(name)));

        cat.deleteAttribute(name);
        assertFalse(cat.attributes().anyMatch(c -> c.equals(name)));
    }

    @Test
    public void whenSettingAndDeletingKeyToType_KeyIsSetAndDeleted() {
        AttributeType<String> username = tx.putAttributeType(Label.of("username"), DataType.STRING);
        person.key(username);
        assertTrue(person.keys().anyMatch(c -> c.equals(username)));

        person.deleteKey(username);
        assertFalse(person.keys().anyMatch(c -> c.equals(username)));
    }

    @Test
    public void whenCallingAddEntity_TypeIsCorrect() {
        Entity newPerson = person.addEntity();
        assertEquals(person, newPerson.type());
    }

    @Test
    public void whenCallingAddRelationship_TypeIsCorrect() {
        Relationship newMarriage = marriage.addRelationship();
        assertEquals(marriage, newMarriage.type());
    }

    @Test
    public void whenCallingPutAttribute_TypeIsCorrect() {
        Attribute<String> nameCharlie = name.putAttribute("Charlie");
        assertEquals(name, nameCharlie.type());
    }

    @Test
    public void whenSettingAndUnsettingRegex_RegexIsSetAndUnset() {
        email.setRegex(null);
        assertNull((email.getRegex()));

        email.setRegex(EMAIL_REGEX);
        assertEquals(EMAIL_REGEX, email.getRegex());
    }

    @Test
    public void whenCallingAddAttributeRelationshipOnThing_RelationshipIsImplicit() {
        assertTrue(alice.attributeRelationship(emailAlice).type().isImplicit());
        assertTrue(alice.attributeRelationship(nameAlice).type().isImplicit());
        assertTrue(alice.attributeRelationship(age20).type().isImplicit());
        assertTrue(bob.attributeRelationship(emailBob).type().isImplicit());
        assertTrue(bob.attributeRelationship(nameBob).type().isImplicit());
        assertTrue(bob.attributeRelationship(age20).type().isImplicit());
    }

    @Test
    public void whenCallingDeleteAttribute_ExecuteAConceptMethod() {
        Entity charlie = person.addEntity();
        Attribute<String> nameCharlie = name.putAttribute("Charlie");
        charlie.attribute(nameCharlie);
        assertTrue(charlie.attributes(name).anyMatch(x -> x.equals(nameCharlie)));

        charlie.deleteAttribute(nameCharlie);
        assertFalse(charlie.attributes(name).anyMatch(x -> x.equals(nameCharlie)));
    }

    @Test
    public void whenAddingAndRemovingRolePlayer_RolePlayerIsAddedAndRemoved() {
        Entity dylan = person.addEntity();
        Entity emily = person.addEntity();

        Relationship dylanAndEmily = friendship.addRelationship()
                .addRolePlayer(friend, dylan)
                .addRolePlayer(friend, emily);

        assertThat(dylanAndEmily.rolePlayers().collect(toSet()), containsInAnyOrder(dylan, emily));

        dylanAndEmily.removeRolePlayer(friend, dylan);
        dylanAndEmily.removeRolePlayer(friend, emily);

        assertTrue(dylanAndEmily.rolePlayers().collect(toSet()).isEmpty());
    }
}
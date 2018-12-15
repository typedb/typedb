/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.concept;

import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.common.exception.ErrorMessage.IS_ABSTRACT;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_CASTING;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;

@SuppressWarnings("Duplicates")
public class SchemaMutationIT {
    private Role husband;
    private Role wife;
    private RelationshipType marriage;
    private RelationshipType drives;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private EntityType vehicle;
    private Thing alice;
    private Thing bob;
    private Thing bmw;
    private Role driver;
    private Role driven;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionImpl tx;
    private SessionImpl session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
        husband = tx.putRole("husband");
        wife = tx.putRole("wife");
        driver = tx.putRole("driver");
        driven = tx.putRole("driven");

        marriage = tx.putRelationshipType("marriage").relates(husband).relates(wife);
        drives = tx.putRelationshipType("drives").relates(driven).relates(driver);

        person = tx.putEntityType("person").plays(husband).plays(wife).plays(driver);
        man = tx.putEntityType("man").sup(person);
        woman = tx.putEntityType("woman").sup(person);
        vehicle = tx.putEntityType("vehicle").plays(driven);
        car = tx.putEntityType("car").sup(vehicle);

        alice = woman.create();
        bob = man.create();
        marriage.create().assign(wife, alice).assign(husband, bob);
        bmw = car.create();
        drives.create().assign(driver, alice).assign(driven, bmw);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }


    @Test
    public void whenDeletingPlaysUsedByExistingCasting_Throw() throws InvalidKBException {
        person.unplay(wife);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(woman.label(), alice.id(), wife.label()));

        tx.commit();
    }

    @Test
    public void whenDeletingRelatesUsedByExistingRelation_Throw() throws InvalidKBException {
        marriage.unrelate(husband);
        expectedException.expect(InvalidKBException.class);
        tx.commit();
    }

    @Test
    public void whenChangingSuperTypeAndInstancesNoLongerAllowedToPlayRoles_Throw() throws InvalidKBException {
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.changingSuperWillDisconnectRole(vehicle, person, driven).getMessage());

        car.sup(person);
    }

    @Test
    public void whenChangingTypeWithInstancesToAbstract_Throw() throws InvalidKBException {
        man.create();

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(man.label()));

        man.isAbstract(true);
    }

    @Test
    public void whenAddingResourceToSubTypeOfEntityType_EnsureNoValidationErrorsOccur(){
        //Create initial Schema
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        EntityType person = tx.putEntityType("perspn").has(name);
        EntityType animal = tx.putEntityType("animal").sup(person);
        Attribute bob = name.create("Bob");
        person.create().has(bob);
        tx.commit();

        //Now make animal have the same resource type
        tx = session.transaction(Transaction.Type.WRITE);
        animal.has(name);
        tx.commit();
    }

    @Test
    public void whenDeletingRelationTypeAndLeavingRoleByItself_Thow(){
        Role role = tx.putRole("my wonderful role");
        RelationshipType relation = tx.putRelationshipType("my wonderful relation").relates(role);
        relation.relates(role);
        tx.commit();

        //Now delete the relation
        tx = session.transaction(Transaction.Type.WRITE);
        relation.delete();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.label())));

        tx.commit();
    }

    @Test
    public void whenChangingTheSuperTypeOfAnEntityTypeWhichHasAResource_EnsureTheResourceIsStillAccessibleViaTheRelationTypeInstances_ByPreventingChange(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);

        //Create a animal and allow animal to have a name
        EntityType animal = tx.putEntityType("animal").has(name);

        //Create a dog which is a animal and is therefore allowed to have a name
        EntityType dog = tx.putEntityType("dog").sup(animal);
        RelationshipType has_name = tx.getRelationshipType("@has-name");

        //Create a dog and name it puppy
        Attribute<String> puppy = name.create("puppy");
        dog.create().has(puppy);

        //Get The Relationship which says that our dog is name puppy
        Relationship expectedEdge = Iterables.getOnlyElement(has_name.instances().collect(toSet()));
        Role hasNameOwner = tx.getRole("@has-name-owner");

        assertThat(expectedEdge.type().instances().collect(toSet()), hasItem(expectedEdge));

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.changingSuperWillDisconnectRole(animal, tx.getMetaEntityType(), hasNameOwner).getMessage());

        //make a dog to not be an animal, and expect exception thrown
        dog.sup(tx.getMetaEntityType());
    }
}
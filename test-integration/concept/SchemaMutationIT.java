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

import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
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

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
        Role husband = tx.putRole("husband");
        Role wife = tx.putRole("wife");
        Role driver = tx.putRole("driver");
        Role driven = tx.putRole("driven");

        RelationType marriage = tx.putRelationType("marriage").relates(husband).relates(wife);
        RelationType drives = tx.putRelationType("drives").relates(driven).relates(driver);

        EntityType person = tx.putEntityType("person").plays(husband).plays(wife).plays(driver);
        EntityType man = tx.putEntityType("man").sup(person);
        EntityType woman = tx.putEntityType("woman").sup(person);
        EntityType vehicle = tx.putEntityType("vehicle").plays(driven);
        EntityType car = tx.putEntityType("car").sup(vehicle);

        Entity alice = woman.create();
        Entity bob = man.create();
        marriage.create().assign(wife, alice).assign(husband, bob);
        Entity bmw = car.create();
        drives.create().assign(driver, alice).assign(driven, bmw);
        tx.commit();
        tx = session.writeTransaction();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    @Test
    public void whenDeletingPlaysUsedByExistingCasting_Throw() throws InvalidKBException {
        Role wife = tx.getRole("wife");
        tx.getEntityType("person").unplay(wife);
        EntityType woman = tx.getEntityType("woman");
        Entity alice = woman.instances().findFirst().get();
        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(woman.label(), alice.id(), wife.label()));

        tx.commit();
    }

    @Test
    public void whenDeletingRelatesUsedByExistingRelation_Throw() throws InvalidKBException {
        RelationType marriage = tx.getRelationType("marriage");
        Role husband = tx.getRole("husband");

        marriage.unrelate(husband);
        expectedException.expect(InvalidKBException.class);
        tx.commit();
    }

    @Test
    public void whenChangingSuperTypeAndInstancesNoLongerAllowedToPlayRoles_Throw() throws InvalidKBException {
        EntityType vehicle = tx.getEntityType("vehicle");
        EntityType person = tx.getEntityType("person");
        Role driven = tx.getRole("driven");
        EntityType car = tx.getEntityType("car");


        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.changingSuperWillDisconnectRole(vehicle, person, driven).getMessage());

        car.sup(person);
    }

    @Test
    public void whenChangingTypeWithInstancesToAbstract_Throw() throws InvalidKBException {
        EntityType man = tx.getEntityType("man");
        man.create();

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(man.label()));

        man.isAbstract(true);
    }

    @Test
    public void whenAddingResourceToSubTypeOfEntityType_EnsureNoValidationErrorsOccur() {
        //Create initial Schema
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        EntityType person = tx.putEntityType("person").has(name);
        EntityType animal = tx.putEntityType("animal").sup(person);
        Attribute bob = name.create("Bob");
        person.create().has(bob);
        tx.commit();

        //Now make animal have the same resource type
        tx = session.writeTransaction();
        EntityType retrievedAnimal = tx.getEntityType("animal");
        AttributeType nameType = tx.getAttributeType("name");
        retrievedAnimal.has(nameType);
        tx.commit();
    }

    @Test
    public void whenDeletingRelationTypeAndLeavingRoleByItself_Throw() {
        Role role = tx.putRole("my wonderful role");
        RelationType relation = tx.putRelationType("my wonderful relation").relates(role);
        relation.relates(role);
        tx.commit();

        //Now delete the relation
        tx = session.writeTransaction();
        RelationType relationInNewTx = tx.getRelationType("my wonderful relation");
        relationInNewTx.delete();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.label())));

        tx.commit();
    }

    @Test
    public void whenChangingTheSuperTypeOfAnEntityTypeWhichHasAResource_EnsureTheResourceIsStillAccessibleViaTheRelationTypeInstances_ByPreventingChange() {
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);

        //Create a animal and allow animal to have a name
        EntityType animal = tx.putEntityType("animal").has(name);

        //Create a dog which is a animal and is therefore allowed to have a name
        EntityType dog = tx.putEntityType("dog").sup(animal);
        RelationType has_name = tx.getRelationType("@has-name");

        //Create a dog and name it puppy
        Attribute<String> puppy = name.create("puppy");
        dog.create().has(puppy);

        //Get The Relation which says that our dog is name puppy
        Relation expectedEdge = Iterables.getOnlyElement(has_name.instances().collect(toSet()));
        Role hasNameOwner = tx.getRole("@has-name-owner");

        assertThat(expectedEdge.type().instances().collect(toSet()), hasItem(expectedEdge));

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.changingSuperWillDisconnectRole(animal, tx.getMetaEntityType(), hasNameOwner).getMessage());

        //make a dog to not be an animal, and expect exception thrown
        dog.sup(tx.getMetaEntityType());
    }
}
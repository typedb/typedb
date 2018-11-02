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

package ai.grakn.kb.internal.concept;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.GraknServer;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;

public class SchemaMutationIT {
    private Role husband;
    private Role wife;
    private RelationshipType marriage;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private Thing alice;
    private Thing bob;
    private Role driver;

    @ClassRule
    public static final GraknServer server = new GraknServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private EmbeddedGraknTx tx;
    private EmbeddedGraknSession session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(GraknTxType.WRITE);
        husband = tx.putRole("Husband");
        wife = tx.putRole("Wife");
        driver = tx.putRole("Driver");
        Role driven = tx.putRole("Driven");

        marriage = tx.putRelationshipType("marriage").relates(husband).relates(wife);
        tx.putRelationshipType("car being driven by").relates(driven).relates(driver);

        person = tx.putEntityType("Person").plays(husband).plays(wife);
        man = tx.putEntityType("Man").sup(person);
        woman = tx.putEntityType("Woman").sup(person);
        car = tx.putEntityType("Car");

        alice = woman.create();
        bob = man.create();
        marriage.create().assign(wife, alice).assign(husband, bob);
        tx.commit();
        tx = session.transaction(GraknTxType.WRITE);
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
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.changingSuperWillDisconnectRole(person, car, wife).getMessage());

        man.sup(car);
    }

    @Test
    public void whenChangingTypeWithInstancesToAbstract_Throw() throws InvalidKBException {
        man.create();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(man.label()));

        man.isAbstract(true);
    }

    @Test
    public void whenAddingEntityTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        tx.close();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void whenAddingRoleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());
        tx.close();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        graknGraphBatch.putRole("This Will Fail");
    }

    @Test
    public void whenAddingResourceTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        tx.close();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        graknGraphBatch.putAttributeType("This Will Fail", AttributeType.DataType.STRING);
    }

    @Test
    public void whenAddingRelationTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());


        tx.close();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        graknGraphBatch.putRelationshipType("This Will Fail");
    }

    @Test
    public void whenAddingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        tx.putRelationshipType(relationTypeId).relates(tx.putRole(roleTypeId));
        tx.commit();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        Role role = graknGraphBatch.getRole(roleTypeId);
        RelationshipType relationshipType = graknGraphBatch.getRelationshipType(relationTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        relationshipType.relates(role);
    }

    @Test
    public void whenAddingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        tx.putEntityType(entityTypeId);
        tx.putRelationshipType("reltype").relates(tx.putRole(roleTypeId));
        tx.commit();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        Role role = graknGraphBatch.getRole(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        entityType.plays(role);
    }

    @Test
    public void whenChangingSuperTypesUsingBatchGraph_Throw(){
        String entityTypeId1 = "entityType1";
        String entityTypeId2 = "entityType2";

        tx.putEntityType(entityTypeId1);
        tx.putEntityType(entityTypeId2);

        tx.commit();
        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        EntityType entityType1 = graknGraphBatch.getEntityType(entityTypeId1);
        EntityType entityType2 = graknGraphBatch.getEntityType(entityTypeId2);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        entityType1.sup(entityType2);
    }


    @Test
    public void whenDeletingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        Role role = tx.putRole(roleTypeId);
        tx.putRelationshipType("reltype").relates(role);
        tx.putEntityType(entityTypeId).plays(role);
        tx.commit();

        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        role = graknGraphBatch.getRole(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        entityType.unplay(role);
    }

    @Test
    public void whenDeletingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        Role role = tx.putRole(roleTypeId);
        tx.putRelationshipType(relationTypeId).relates(role);
        tx.commit();

        EmbeddedGraknTx<?> graknGraphBatch = session.transaction(GraknTxType.BATCH);
        role = graknGraphBatch.getRole(roleTypeId);
        RelationshipType relationshipType = graknGraphBatch.getRelationshipType(relationTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        relationshipType.unrelate(role);
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
        tx = session.transaction(GraknTxType.WRITE);
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
        tx = session.transaction(GraknTxType.WRITE);
        relation.delete();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.label())));

        tx.commit();
    }

    @Test
    public void whenChangingTheSuperTypeOfAnEntityTypeWhichHasAResource_EnsureTheResourceIsStillAccessibleViaTheRelationTypeInstances_ByPreventingChange(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);

        //Create a person and allow person to have a name
        EntityType person = tx.putEntityType("person").has(name);

        //Create a man which is a person and is therefore allowed to have a name
        EntityType man = tx.putEntityType("man").sup(person);
        RelationshipType has_name = tx.getRelationshipType("@has-name");

        //Create a Man and name him Bob
        Attribute<String> nameBob = name.create("Bob");
        man.create().has(nameBob);

        //Get The Relationship which says that our man is name bob
        Relationship expectedEdge = Iterables.getOnlyElement(has_name.instances().collect(toSet()));
        Role hasNameOwner = tx.getRole("@has-name-owner");

        assertThat(expectedEdge.type().instances().collect(toSet()), hasItem(expectedEdge));

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.changingSuperWillDisconnectRole(person, tx.admin().getMetaEntityType(), hasNameOwner).getMessage());

        //Man is no longer a person and therefore is not allowed to have a name
        man.sup(tx.admin().getMetaEntityType());
    }
}
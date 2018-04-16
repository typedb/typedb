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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.concept;

/*-
 * #%L
 * grakn-kb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.Grakn;
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
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;

public class SchemaMutationTest extends TxTestBase {
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

    @Before
    public void setup() throws InvalidKBException {
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

        alice = woman.addEntity();
        bob = man.addEntity();
        marriage.addRelationship().addRolePlayer(wife, alice).addRolePlayer(husband, bob);
        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).open(GraknTxType.WRITE);
    }

    @Test
    public void whenDeletingPlaysUsedByExistingCasting_Throw() throws InvalidKBException {
        person.deletePlays(wife);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(woman.getLabel(), alice.getId(), wife.getLabel()));

        tx.commit();
    }

    @Test
    public void whenDeletingRelatesUsedByExistingRelation_Throw() throws InvalidKBException {
        marriage.deleteRelates(husband);
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
        man.addEntity();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(man.getLabel()));

        man.setAbstract(true);
    }

    @Test
    public void whenAddingEntityTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void whenAddingRoleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        graknGraphBatch.putRole("This Will Fail");
    }

    @Test
    public void whenAddingResourceTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        graknGraphBatch.putAttributeType("This Will Fail", AttributeType.DataType.STRING);
    }

    @Test
    public void whenAddingRelationTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        graknGraphBatch.putRelationshipType("This Will Fail");
    }

    @Test
    public void whenAddingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        tx.putRole(roleTypeId);
        tx.putRelationshipType(relationTypeId);

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
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
        tx.putRole(roleTypeId);
        tx.putEntityType(entityTypeId);

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
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

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
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
        tx.putEntityType(entityTypeId).plays(role);

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        role = graknGraphBatch.getRole(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        entityType.deletePlays(role);
    }

    @Test
    public void whenDeletingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        Role role = tx.putRole(roleTypeId);
        tx.putRelationshipType(relationTypeId).relates(role);
        tx.commit();

        EmbeddedGraknTx<?> graknGraphBatch = batchTx();
        role = graknGraphBatch.getRole(roleTypeId);
        RelationshipType relationshipType = graknGraphBatch.getRelationshipType(relationTypeId);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.schemaMutation().getMessage());

        relationshipType.deleteRelates(role);
    }

    @Test
    public void whenAddingResourceToSubTypeOfEntityType_EnsureNoValidationErrorsOccur(){
        //Create initial Schema
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        EntityType person = tx.putEntityType("perspn").attribute(name);
        EntityType animal = tx.putEntityType("animal").sup(person);
        Attribute bob = name.putAttribute("Bob");
        person.addEntity().attribute(bob);
        tx.commit();

        //Now make animal have the same resource type
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).open(GraknTxType.WRITE);
        animal.attribute(name);
        tx.commit();
    }

    @Test
    public void whenDeletingRelationTypeAndLeavingRoleByItself_Thow(){
        Role role = tx.putRole("my wonderful role");
        RelationshipType relation = tx.putRelationshipType("my wonderful relation").relates(role);
        relation.relates(role);
        tx.commit();

        //Now delete the relation
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).open(GraknTxType.WRITE);
        relation.delete();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.getLabel())));

        tx.commit();
    }

    @Test
    public void whenChangingTheSuperTypeOfAnEntityTypeWhichHasAResource_EnsureTheResourceIsStillAccessibleViaTheRelationTypeInstances_ByPreventingChange(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);

        //Create a person and allow person to have a name
        EntityType person = tx.putEntityType("person").attribute(name);

        //Create a man which is a person and is therefore allowed to have a name
        EntityType man = tx.putEntityType("man").sup(person);
        RelationshipType has_name = tx.getRelationshipType("@has-name");

        //Create a Man and name him Bob
        Attribute<String> nameBob = name.putAttribute("Bob");
        man.addEntity().attribute(nameBob);

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

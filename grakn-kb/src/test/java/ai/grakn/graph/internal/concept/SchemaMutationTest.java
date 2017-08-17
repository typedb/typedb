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

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.GraknTxAbstract;
import ai.grakn.graph.internal.GraphTestBase;
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

public class SchemaMutationTest extends GraphTestBase {
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
    public void buildMarriageGraph() throws InvalidGraphException {
        husband = graknGraph.putRole("Husband");
        wife = graknGraph.putRole("Wife");
        driver = graknGraph.putRole("Driver");
        Role driven = graknGraph.putRole("Driven");

        marriage = graknGraph.putRelationshipType("marriage").relates(husband).relates(wife);
        graknGraph.putRelationshipType("car being driven by").relates(driven).relates(driver);

        person = graknGraph.putEntityType("Person").plays(husband).plays(wife);
        man = graknGraph.putEntityType("Man").sup(person);
        woman = graknGraph.putEntityType("Woman").sup(person);
        car = graknGraph.putEntityType("Car");

        alice = woman.addEntity();
        bob = man.addEntity();
        marriage.addRelationship().addRolePlayer(wife, alice).addRolePlayer(husband, bob);
        graknGraph.commit();
        graknGraph = (GraknTxAbstract<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
    }

    @Test
    public void whenDeletingPlaysUsedByExistingCasting_Throw() throws InvalidGraphException {
        person.deletePlays(wife);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(woman.getLabel(), alice.getId(), wife.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelatesUsedByExistingRelation_Throw() throws InvalidGraphException {
        marriage.deleteRelates(husband);
        expectedException.expect(InvalidGraphException.class);
        graknGraph.commit();
    }

    @Test
    public void whenChangingSuperTypeAndInstancesNoLongerAllowedToPlayRoles_Throw() throws InvalidGraphException {
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.changingSuperWillDisconnectRole(person, car, driver).getMessage());

        man.sup(car);
    }

    @Test
    public void whenChangingTypeWithInstancesToAbstract_Throw() throws InvalidGraphException {
        man.addEntity();

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(man.getLabel()));

        man.setAbstract(true);
    }

    @Test
    public void whenAddingEntityTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void whenAddingRoleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRole("This Will Fail");
    }

    @Test
    public void whenAddingResourceTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putAttributeType("This Will Fail", AttributeType.DataType.STRING);
    }

    @Test
    public void whenAddingRuleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRuleType("This Will Fail");
    }

    @Test
    public void whenAddingRelationTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRelationshipType("This Will Fail");
    }

    @Test
    public void whenAddingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        graknGraph.putRole(roleTypeId);
        graknGraph.putRelationshipType(relationTypeId);

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        Role role = graknGraphBatch.getRole(roleTypeId);
        RelationshipType relationshipType = graknGraphBatch.getRelationshipType(relationTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        relationshipType.relates(role);
    }

    @Test
    public void whenAddingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        graknGraph.putRole(roleTypeId);
        graknGraph.putEntityType(entityTypeId);

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        Role role = graknGraphBatch.getRole(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        entityType.plays(role);
    }

    @Test
    public void whenChangingSuperTypesUsingBatchGraph_Throw(){
        String entityTypeId1 = "entityType1";
        String entityTypeId2 = "entityType2";

        graknGraph.putEntityType(entityTypeId1);
        graknGraph.putEntityType(entityTypeId2);

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        EntityType entityType1 = graknGraphBatch.getEntityType(entityTypeId1);
        EntityType entityType2 = graknGraphBatch.getEntityType(entityTypeId2);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        entityType1.sup(entityType2);
    }


    @Test
    public void whenDeletingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        Role role = graknGraph.putRole(roleTypeId);
        graknGraph.putEntityType(entityTypeId).plays(role);

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        role = graknGraphBatch.getRole(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        entityType.deletePlays(role);
    }

    @Test
    public void whenDeletingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        Role role = graknGraph.putRole(roleTypeId);
        graknGraph.putRelationshipType(relationTypeId).relates(role);
        graknGraph.commit();

        GraknTxAbstract<?> graknGraphBatch = switchToBatchGraph();
        role = graknGraphBatch.getRole(roleTypeId);
        RelationshipType relationshipType = graknGraphBatch.getRelationshipType(relationTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        relationshipType.deleteRelates(role);
    }

    @Test
    public void whenAddingResourceToSubTypeOfEntityType_EnsureNoValidationErrorsOccur(){
        //Create initial Ontology
        AttributeType<String> name = graknGraph.putAttributeType("name", AttributeType.DataType.STRING);
        EntityType person = graknGraph.putEntityType("perspn").attribute(name);
        EntityType animal = graknGraph.putEntityType("animal").sup(person);
        Attribute bob = name.putAttribute("Bob");
        person.addEntity().attribute(bob);
        graknGraph.commit();

        //Now make animal have the same resource type
        graknGraph = (GraknTxAbstract) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
        animal.attribute(name);
        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelationTypeAndLeavingRoleByItself_Thow(){
        Role role = graknGraph.putRole("my wonderful role");
        RelationshipType relation = graknGraph.putRelationshipType("my wonderful relation").relates(role);
        relation.relates(role);
        graknGraph.commit();

        //Now delete the relation
        graknGraph = (GraknTxAbstract) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
        relation.delete();

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.getLabel())));

        graknGraph.commit();
    }

    @Test
    public void whenChangingTheSuperTypeOfAnEntityTypeWhichHasAResource_EnsureTheResourceIsStillAccessibleViaTheRelationTypeInstances_ByPreventingChange(){
        AttributeType<String> name = graknGraph.putAttributeType("name", AttributeType.DataType.STRING);

        //Create a person and allow person to have a name
        EntityType person = graknGraph.putEntityType("person").attribute(name);

        //Create a man which is a person and is therefore allowed to have a name
        EntityType man = graknGraph.putEntityType("man").sup(person);
        RelationshipType has_name = graknGraph.putRelationshipType("has-name");

        //Create a Man and name him Bob
        Attribute<String> nameBob = name.putAttribute("Bob");
        man.addEntity().attribute(nameBob);

        //Get The Relationship which says that our man is name bob
        Relationship expectedEdge = Iterables.getOnlyElement(has_name.instances().collect(toSet()));
        Role hasNameOwner = graknGraph.getRole("has-name-owner");

        assertThat(expectedEdge.type().instances().collect(toSet()), hasItem(expectedEdge));

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.changingSuperWillDisconnectRole(person, graknGraph.admin().getMetaEntityType(), hasNameOwner).getMessage());

        //Man is no longer a person and therefore is not allowed to have a name
        man.sup(graknGraph.admin().getMetaEntityType());
    }
}

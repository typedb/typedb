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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.util.ErrorMessage;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;

public class OntologyMutationTest extends GraphTestBase {
    private Role husband;
    private Role wife;
    private RelationType marriage;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private Thing alice;
    private Thing bob;

    @Before
    public void buildMarriageGraph() throws InvalidGraphException {
        husband = graknGraph.putRole("Husband");
        wife = graknGraph.putRole("Wife");
        Role driver = graknGraph.putRole("Driver");
        Role driven = graknGraph.putRole("Driven");

        marriage = graknGraph.putRelationType("marriage").relates(husband).relates(wife);
        graknGraph.putRelationType("car being driven by").relates(driven).relates(driver);

        person = graknGraph.putEntityType("Person").plays(husband).plays(wife);
        man = graknGraph.putEntityType("Man").sup(person);
        woman = graknGraph.putEntityType("Woman").sup(person);
        car = graknGraph.putEntityType("Car");

        alice = woman.addEntity();
        bob = man.addEntity();
        marriage.addRelation().addRolePlayer(wife, alice).addRolePlayer(husband, bob);
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
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
        man.sup(car);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(man.getLabel(), bob.getId(), husband.getLabel()));

        graknGraph.commit();
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

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void whenAddingRoleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRole("This Will Fail");
    }

    @Test
    public void whenAddingResourceTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putResourceType("This Will Fail", ResourceType.DataType.STRING);
    }

    @Test
    public void whenAddingRuleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRuleType("This Will Fail");
    }

    @Test
    public void whenAddingRelationTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRelationType("This Will Fail");
    }

    @Test
    public void whenAddingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        graknGraph.putRole(roleTypeId);
        graknGraph.putRelationType(relationTypeId);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        Role role = graknGraphBatch.getRole(roleTypeId);
        RelationType relationType = graknGraphBatch.getRelationType(relationTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        relationType.relates(role);
    }

    @Test
    public void whenAddingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        graknGraph.putRole(roleTypeId);
        graknGraph.putEntityType(entityTypeId);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
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

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
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

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
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
        graknGraph.putRelationType(relationTypeId).relates(role);
        graknGraph.commit();

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        role = graknGraphBatch.getRole(roleTypeId);
        RelationType relationType = graknGraphBatch.getRelationType(relationTypeId);

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.ontologyMutation().getMessage());

        relationType.deleteRelates(role);
    }

    @Test
    public void whenAddingResourceToSubTypeOfEntityType_EnsureNoValidationErrorsOccur(){
        //Create initial Ontology
        ResourceType<String> name = graknGraph.putResourceType("name", ResourceType.DataType.STRING);
        EntityType person = graknGraph.putEntityType("perspn").resource(name);
        EntityType animal = graknGraph.putEntityType("animal").sup(person);
        Resource bob = name.putResource("Bob");
        person.addEntity().resource(bob);
        graknGraph.commit();

        //Now make animal have the same resource type
        graknGraph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
        animal.resource(name);
        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelationTypeAndLeavingRoleByItself_Thow(){
        Role role = graknGraph.putRole("my wonderful role");
        RelationType relation = graknGraph.putRelationType("my wonderful relation").relates(role);
        relation.relates(role);
        graknGraph.commit();

        //Now delete the relation
        graknGraph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
        relation.delete();

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(Matchers.containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.getLabel())));

        graknGraph.commit();
    }
}

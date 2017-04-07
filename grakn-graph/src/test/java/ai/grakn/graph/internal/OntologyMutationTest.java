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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.util.ErrorMessage.SCHEMA_LOCKED;
import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static ai.grakn.util.ErrorMessage.VALIDATION_IS_ABSTRACT;

public class OntologyMutationTest extends GraphTestBase{
    private RoleType husband;
    private RoleType wife;
    private RelationType marriage;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private Instance alice;
    private Instance bob;

    @Before
    public void buildMarriageGraph() throws GraknValidationException {
        husband = graknGraph.putRoleType("Husband");
        wife = graknGraph.putRoleType("Wife");
        RoleType driver = graknGraph.putRoleType("Driver");
        RoleType driven = graknGraph.putRoleType("Driven");

        marriage = graknGraph.putRelationType("marriage").relates(husband).relates(wife);
        graknGraph.putRelationType("car being driven by").relates(driven).relates(driver);

        person = graknGraph.putEntityType("Person").plays(husband).plays(wife);
        man = graknGraph.putEntityType("Man").superType(person);
        woman = graknGraph.putEntityType("Woman").superType(person);
        car = graknGraph.putEntityType("Car");

        alice = woman.addEntity();
        bob = man.addEntity();
        marriage.addRelation().addRolePlayer(wife, alice).addRolePlayer(husband, bob);
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
    }

    @Test
    public void whenDeletingPlaysUsedByExistingCasting_Throw() throws GraknValidationException {
        person.deletePlays(wife);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(woman.getLabel(), alice.getId(), wife.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelatesUsedByExistingRelation_Throw() throws GraknValidationException {
        marriage.deleteRelates(husband);
        expectedException.expect(GraknValidationException.class);
        graknGraph.commit();
    }

    @Test
    public void whenChanginSuperTypeAndInstancesNoLongerAllowedToPlayRoles_Throw() throws GraknValidationException {
        man.superType(car);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(VALIDATION_CASTING.getMessage(man.getLabel(), bob.getId(), husband.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenChangingTypeWithInstancesToAbstract_Throw() throws GraknValidationException {
        man.setAbstract(true);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(VALIDATION_IS_ABSTRACT.getMessage(man.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenAddingEntityTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void whenAddingRoleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRoleType("This Will Fail");
    }

    @Test
    public void whenAddingResourceTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putResourceType("This Will Fail", ResourceType.DataType.STRING);
    }

    @Test
    public void whenAddingRuleTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRuleType("This Will Fail");
    }

    @Test
    public void whenAddingRelationTypeUsingBatchLoadingGraph_Throw(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        graknGraphBatch.putRelationType("This Will Fail");
    }

    @Test
    public void whenAddingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        graknGraph.putRoleType(roleTypeId);
        graknGraph.putRelationType(relationTypeId);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        RoleType roleType = graknGraphBatch.getRoleType(roleTypeId);
        RelationType relationType = graknGraphBatch.getRelationType(relationTypeId);

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        relationType.relates(roleType);
    }

    @Test
    public void whenAddingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        graknGraph.putRoleType(roleTypeId);
        graknGraph.putEntityType(entityTypeId);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        RoleType roleType = graknGraphBatch.getRoleType(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        entityType.plays(roleType);
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

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        entityType1.superType(entityType2);
    }


    @Test
    public void whenDeletingPlaysUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String entityTypeId = "entityType";
        RoleType roleType = graknGraph.putRoleType(roleTypeId);
        graknGraph.putEntityType(entityTypeId).plays(roleType);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        roleType = graknGraphBatch.getRoleType(roleTypeId);
        EntityType entityType = graknGraphBatch.getEntityType(entityTypeId);

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        entityType.deletePlays(roleType);
    }

    @Test
    public void whenDeletingRelatesUsingBatchGraph_Throw(){
        String roleTypeId = "role-thing";
        String relationTypeId = "relationtype";
        RoleType roleType = graknGraph.putRoleType(roleTypeId);
        graknGraph.putRelationType(relationTypeId).relates(roleType);

        AbstractGraknGraph<?> graknGraphBatch = switchToBatchGraph();
        roleType = graknGraphBatch.getRoleType(roleTypeId);
        RelationType relationType = graknGraphBatch.getRelationType(relationTypeId);

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(SCHEMA_LOCKED.getMessage());

        relationType.deleteRelates(roleType);
    }
}

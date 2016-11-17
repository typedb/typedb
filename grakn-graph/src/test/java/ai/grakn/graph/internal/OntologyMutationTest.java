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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;

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
    private Relation relation;

    @Before
    public void buildGraph() throws GraknValidationException {
        husband = graknGraph.putRoleType("Husband");
        wife = graknGraph.putRoleType("Wife");
        RoleType driver = graknGraph.putRoleType("Driver");
        RoleType driven = graknGraph.putRoleType("Driven");

        marriage = graknGraph.putRelationType("marriage").hasRole(husband).hasRole(wife);
        graknGraph.putRelationType("car being driven by").hasRole(driven).hasRole(driver);

        person = graknGraph.putEntityType("Person").playsRole(husband).playsRole(wife);
        man = graknGraph.putEntityType("Man").superType(person);
        woman = graknGraph.putEntityType("Woman").superType(person);
        car = graknGraph.putEntityType("Car");

        alice = woman.addEntity();
        bob = man.addEntity();
        relation = marriage.addRelation().putRolePlayer(wife, alice).putRolePlayer(husband, bob);
        graknGraph.commit();
    }

    @Test
    public void testDeletePlaysRole() throws GraknValidationException {
        person.deletePlaysRole(wife);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(woman.getId(), alice.getId(), wife.getId()))
        ));

        graknGraph.commit();
    }

    @Test
    public void testDeleteHasRole() throws GraknValidationException {
        marriage.deleteHasRole(husband);

        String roles = "";
        String rolePlayers = "";
        for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
            if(entry.getKey() != null)
                roles = roles + entry.getKey().getId() + ",";
            if(entry.getValue() != null)
                rolePlayers = rolePlayers + entry.getValue().getId() + ",";
        }

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), marriage.getId(),
                        roles.split(",").length, roles,
                        rolePlayers.split(",").length, roles))
        ));

        graknGraph.commit();
    }

    @Test
    public void testChangeSuperTypeOfEntityType() throws GraknValidationException {
        man.superType(car);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(man.getId(), bob.getId(), husband.getId()))
        ));

        graknGraph.commit();
    }

    @Test
    public void testChangeIsAbstract() throws GraknValidationException {
        man.setAbstract(true);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(man.getId()))
        ));

        graknGraph.commit();
    }

    @Test
    public void testAddingEntityTypeWhileBatchLoading(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.SCHEMA_LOCKED.getMessage())
        ));

        graknGraphBatch.putEntityType("This Will Fail");
    }

    @Test
    public void testAddingRoleTypeWhileBatchLoading(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.SCHEMA_LOCKED.getMessage())
        ));

        graknGraphBatch.putRoleType("This Will Fail");
    }

    @Test
    public void testAddingResourceTypeWhileBatchLoading(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.SCHEMA_LOCKED.getMessage())
        ));

        graknGraphBatch.putResourceType("This Will Fail", ResourceType.DataType.STRING);
    }

    @Test
    public void testAddingRuleTypeWhileBatchLoading(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.SCHEMA_LOCKED.getMessage())
        ));

        graknGraphBatch.putRuleType("This Will Fail");
    }

    @Test
    public void testAddingRelationTypeWhileBatchLoading(){
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.SCHEMA_LOCKED.getMessage())
        ));

        graknGraphBatch.putRelationType("This Will Fail");
    }
}

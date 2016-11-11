/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.MindmapsValidationException;
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
    public void buildGraph() throws MindmapsValidationException {
        husband = mindmapsGraph.putRoleType("Husband");
        wife = mindmapsGraph.putRoleType("Wife");
        RoleType driver = mindmapsGraph.putRoleType("Driver");
        RoleType driven = mindmapsGraph.putRoleType("Driven");

        marriage = mindmapsGraph.putRelationType("marriage").hasRole(husband).hasRole(wife);
        mindmapsGraph.putRelationType("car being driven by").hasRole(driven).hasRole(driver);

        person = mindmapsGraph.putEntityType("Person").playsRole(husband).playsRole(wife);
        man = mindmapsGraph.putEntityType("Man").superType(person);
        woman = mindmapsGraph.putEntityType("Woman").superType(person);
        car = mindmapsGraph.putEntityType("Car");

        alice = mindmapsGraph.addEntity(woman);
        bob = mindmapsGraph.addEntity(man);
        relation = mindmapsGraph.addRelation(marriage).putRolePlayer(wife, alice).putRolePlayer(husband, bob);
        mindmapsGraph.commit();
    }

    @Test
    public void testDeletePlaysRole() throws MindmapsValidationException {
        person.deletePlaysRole(wife);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(woman.getId(), alice.getId(), wife.getId()))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testDeleteHasRole() throws MindmapsValidationException {
        marriage.deleteHasRole(husband);

        String roles = "";
        String rolePlayers = "";
        for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
            if(entry.getKey() != null)
                roles = roles + entry.getKey().getId() + ",";
            if(entry.getValue() != null)
                rolePlayers = rolePlayers + entry.getValue().getId() + ",";
        }

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), marriage.getId(),
                        roles.split(",").length, roles,
                        rolePlayers.split(",").length, roles))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testChangeSuperTypeOfEntityType() throws MindmapsValidationException {
        man.superType(car);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(man.getId(), bob.getId(), husband.getId()))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testChangeIsAbstract() throws MindmapsValidationException{
        man.setAbstract(true);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(man.getId()))
        ));

        mindmapsGraph.commit();
    }

}

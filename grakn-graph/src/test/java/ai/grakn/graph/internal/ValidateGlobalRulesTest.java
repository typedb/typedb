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

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graph.internal.concept.EntityImpl;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.concept.RelationImpl;
import ai.grakn.graph.internal.concept.RoleImpl;
import ai.grakn.graph.internal.concept.ThingImpl;
import ai.grakn.graph.internal.structure.Casting;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesTest extends GraphTestBase{

    @Test
    public void testValidatePlaysStructure() throws Exception {
        EntityTypeImpl wolf = (EntityTypeImpl) graknGraph.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) graknGraph.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) graknGraph.putEntityType("hunter");
        RelationType hunts = graknGraph.putRelationType("hunts");
        RoleImpl witcher = (RoleImpl) graknGraph.putRole("witcher");
        RoleImpl monster = (RoleImpl) graknGraph.putRole("monster");
        Thing geralt = hunter.addEntity();
        ThingImpl werewolf = (ThingImpl) wolf.addEntity();

        RelationImpl assertion = (RelationImpl) hunts.addRelation().
                addRolePlayer(witcher, geralt).addRolePlayer(monster, werewolf);
        assertion.reified().get().castingsRelation().forEach(rolePlayer ->
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent()));

        hunter.plays(witcher);

        boolean [] flags = {false, false};
        int count = 0;

        for (Casting casting : assertion.reified().get().castingsRelation().collect(Collectors.toSet())) {
            flags[count] = ValidateGlobalRules.validatePlaysStructure(casting).isPresent();
            count++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        wolf.sup(creature);
        creature.plays(monster);

        for (Casting casting : assertion.reified().get().castingsRelation().collect(Collectors.toSet())) {
            assertFalse(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
        }
    }

    @Test
    public void testValidatePlaysStructureUnique() {
        Role role1 = graknGraph.putRole("role1");
        Role role2 = graknGraph.putRole("role2");
        RelationType relationType = graknGraph.putRelationType("rt").relates(role1).relates(role2);

        EntityType entityType = graknGraph.putEntityType("et");

        ((EntityTypeImpl) entityType).plays(role1, true);
        ((EntityTypeImpl) entityType).plays(role2, false);

        Entity other1 = entityType.addEntity();
        Entity other2 = entityType.addEntity();

        EntityImpl entity = (EntityImpl) entityType.addEntity();

        RelationImpl relation1 = (RelationImpl) relationType.addRelation()
                .addRolePlayer(role2, other1).addRolePlayer(role1, entity);

        // Valid with only a single relation
        relation1.reified().get().castingsRelation().forEach(rolePlayer ->
                assertFalse(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent()));

        RelationImpl relation2 = (RelationImpl) relationType.addRelation()
                .addRolePlayer(role2, other2).addRolePlayer(role1, entity);

        // Invalid with multiple relations
        relation1.reified().get().castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRoleType().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent());
            }
        });
        relation2.reified().get().castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRoleType().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent());
            }
        });
    }

    @Test
    public void testValidateRelationTypeRelates() throws Exception {
        Role hunter = graknGraph.putRole("hunter");
        RelationType kills = graknGraph.putRelationType("kills");

        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.relates(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
    }

    @Test
    public void testValidateAssertionStructure() throws Exception {
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        Role napper = graknGraph.putRole("napper");
        Role hunter = graknGraph.putRole("hunter");
        Role monster = graknGraph.putRole("monster");
        Role creature = graknGraph.putRole("creature");
        Thing cthulhu = fakeType.addEntity();
        Thing werewolf = fakeType.addEntity();
        Thing cartman = fakeType.addEntity();
        RelationType kills = graknGraph.putRelationType("kills");
        RelationType naps = graknGraph.putRelationType("naps").relates(napper);

        RelationImpl assertion = (RelationImpl) kills.addRelation().
                addRolePlayer(hunter, cartman).addRolePlayer(monster, werewolf).addRolePlayer(creature, cthulhu);

        kills.relates(monster);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion.reified().get()).isPresent());

        kills.relates(hunter);
        kills.relates(creature);
        assertFalse(ValidateGlobalRules.validateRelationshipStructure(assertion.reified().get()).isPresent());

        RelationImpl assertion2 = (RelationImpl) naps.addRelation().addRolePlayer(hunter, cthulhu);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion2.reified().get()).isPresent());
    }


    @Test
    public void testAbstractConceptValidation(){
        Role role = graknGraph.putRole("relates");
        RelationType relationType = graknGraph.putRelationType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());

        relationType.setAbstract(true);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());
    }
}
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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
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
        RoleTypeImpl witcher = (RoleTypeImpl) graknGraph.putRoleType("witcher");
        RoleTypeImpl monster = (RoleTypeImpl) graknGraph.putRoleType("monster");
        Instance geralt = hunter.addEntity();
        InstanceImpl werewolf = (InstanceImpl) wolf.addEntity();

        RelationImpl assertion = (RelationImpl) hunts.addRelation().
                addRolePlayer(witcher, geralt).addRolePlayer(monster, werewolf);
        assertion.castingsRelation().forEach(rolePlayer ->
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent()));

        hunter.plays(witcher);

        boolean [] flags = {false, false};
        int count = 0;

        for (Casting casting : assertion.castingsRelation().collect(Collectors.toSet())) {
            flags[count] = ValidateGlobalRules.validatePlaysStructure(casting).isPresent();
            count++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        wolf.superType(creature);
        creature.plays(monster);

        for (Casting casting : assertion.castingsRelation().collect(Collectors.toSet())) {
            assertFalse(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
        }
    }

    @Test
    public void testValidatePlaysStructureUnique() {
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
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
        relation1.castingsRelation().forEach(rolePlayer ->
                assertFalse(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent()));

        RelationImpl relation2 = (RelationImpl) relationType.addRelation()
                .addRolePlayer(role2, other2).addRolePlayer(role1, entity);

        // Invalid with multiple relations
        relation1.castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRoleType().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent());
            }
        });
        relation2.castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRoleType().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(rolePlayer).isPresent());
            }
        });
    }

    @Test
    public void testValidateRelationTypeRelates() throws Exception {
        RoleType hunter = graknGraph.putRoleType("hunter");
        RelationType kills = graknGraph.putRelationType("kills");

        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.relates(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
    }

    @Test
    public void testValidateAssertionStructure() throws Exception {
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RoleType napper = graknGraph.putRoleType("napper");
        RoleType hunter = graknGraph.putRoleType("hunter");
        RoleType monster = graknGraph.putRoleType("monster");
        RoleType creature = graknGraph.putRoleType("creature");
        Instance cthulhu = fakeType.addEntity();
        Instance werewolf = fakeType.addEntity();
        Instance cartman = fakeType.addEntity();
        RelationType kills = graknGraph.putRelationType("kills");
        RelationType naps = graknGraph.putRelationType("naps").relates(napper);

        RelationImpl assertion = (RelationImpl) kills.addRelation().
                addRolePlayer(hunter, cartman).addRolePlayer(monster, werewolf).addRolePlayer(creature, cthulhu);

        kills.relates(monster);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion).isPresent());

        kills.relates(hunter);
        kills.relates(creature);
        assertFalse(ValidateGlobalRules.validateRelationshipStructure(assertion).isPresent());

        RelationImpl assertion2 = (RelationImpl) naps.addRelation().addRolePlayer(hunter, cthulhu);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion2).isPresent());
    }


    @Test
    public void testAbstractConceptValidation(){
        RoleType roleType = graknGraph.putRoleType("relates");
        RelationType relationType = graknGraph.putRelationType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(roleType).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());

        roleType.setAbstract(true);
        relationType.setAbstract(true);

        assertFalse(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(roleType).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());
    }
}
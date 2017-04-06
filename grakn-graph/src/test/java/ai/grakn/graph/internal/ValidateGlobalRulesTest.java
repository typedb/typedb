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
import ai.grakn.concept.Type;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesTest extends GraphTestBase{

    @Test(expected = InvocationTargetException.class)
    public void testConstructor() throws Exception { //Checks that you cannot initialise it.
        Constructor<ValidateGlobalRules> c = ValidateGlobalRules.class.getDeclaredConstructor();
        c.setAccessible(true);
        ValidateGlobalRules u = c.newInstance();
    }


    @Test
    public void testValidatePlaysStructure() throws Exception {
        Type fakeType = graknGraph.putEntityType("Fake Concept");
        EntityTypeImpl wolf = (EntityTypeImpl) graknGraph.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) graknGraph.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) graknGraph.putEntityType("hunter");
        RoleType animal = graknGraph.putRoleType("animal");
        RelationType hunts = graknGraph.putRelationType("hunts");
        RoleTypeImpl witcher = (RoleTypeImpl) graknGraph.putRoleType("witcher");
        RoleTypeImpl monster = (RoleTypeImpl) graknGraph.putRoleType("monster");
        Instance geralt = hunter.addEntity();
        InstanceImpl werewolf = (InstanceImpl) wolf.addEntity();

        RelationImpl assertion = (RelationImpl) hunts.addRelation().
                addRolePlayer(witcher, geralt).addRolePlayer(monster, werewolf);
        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertTrue(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
        }

        hunter.plays(witcher);

        boolean [] flags = {false, false};
        int count = 0;
        for (CastingImpl casting : assertion.getMappingCasting()) {
            flags[count] = ValidateGlobalRules.validatePlaysStructure(casting).isPresent();
            count++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        wolf.superType(creature);
        creature.plays(monster);

        for (CastingImpl casting : assertion.getMappingCasting()) {
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
        relation1.getMappingCasting().forEach(casting -> {
            assertFalse(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
        });

        RelationImpl relation2 = (RelationImpl) relationType.addRelation()
                .addRolePlayer(role2, other2).addRolePlayer(role1, entity);

        // Invalid with multiple relations
        relation1.getMappingCasting().forEach(casting -> {
            if (casting.getRole().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
            }
        });
        relation2.getMappingCasting().forEach(casting -> {
            if (casting.getRole().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysStructure(casting).isPresent());
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

    @Test
    public void testAbstractInstancesDoNotValidateSubTypes(){
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType entityType = graknGraph.putEntityType("entityType").plays(r1).plays(r2);
        RelationType relationType = graknGraph.putRelationType("relationTypes").setAbstract(true);
        RelationType hasCast = graknGraph.putRelationType("has cast").superType(relationType).relates(r1).relates(r2);

        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        hasCast.addRelation().addRolePlayer(r1, e1).addRolePlayer(r2, e2);

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges((TypeImpl) relationType).isPresent());
    }

    @Test
    public void validateIsAbstractHasNoIncomingIsaEdges() {
        EntityTypeImpl x1 = (EntityTypeImpl) graknGraph.putEntityType("x1");
        EntityTypeImpl x2 = (EntityTypeImpl) graknGraph.putEntityType("x2'");
        EntityTypeImpl x3 = (EntityTypeImpl) graknGraph.putEntityType("x3");
        EntityTypeImpl x4 = (EntityTypeImpl) graknGraph.putEntityType("x4'");

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x2).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x3).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x4).isPresent());

        x2.superType(x1);

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1).isPresent());
    }
}
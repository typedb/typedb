/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesTest {
    private MindmapsTransactionImpl mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
    }

    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsGraph.close();
    }

    @Test(expected = InvocationTargetException.class)
    public void testConstructor() throws Exception { //Checks that you cannot initialise it.
        Constructor<ValidateGlobalRules> c = ValidateGlobalRules.class.getDeclaredConstructor();
        c.setAccessible(true);
        ValidateGlobalRules u = c.newInstance();
    }


    @Test
    public void testValidatePlaysRoleStructure() throws Exception {
        Type fakeType = mindmapsGraph.putEntityType("Fake Concept");
        EntityTypeImpl wolf = (EntityTypeImpl) mindmapsGraph.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) mindmapsGraph.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) mindmapsGraph.putEntityType("hunter");
        RoleType animal = mindmapsGraph.putRoleType("animal");
        RelationType hunts = mindmapsGraph.putRelationType("hunts");
        RoleTypeImpl witcher = (RoleTypeImpl) mindmapsGraph.putRoleType("witcher");
        RoleTypeImpl monster = (RoleTypeImpl) mindmapsGraph.putRoleType("monster");
        Instance geralt = mindmapsGraph.putEntity("geralt", hunter);
        InstanceImpl werewolf = (InstanceImpl) mindmapsGraph.putEntity("werewolf", wolf);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), hunts).
                putRolePlayer(witcher, geralt).putRolePlayer(monster, werewolf);
        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertFalse(ValidateGlobalRules.validatePlaysRoleStructure(casting));
        }

        hunter.playsRole(witcher);

        boolean [] flags = {false, false};
        int count = 0;
        for (CastingImpl casting : assertion.getMappingCasting()) {
            flags[count] = ValidateGlobalRules.validatePlaysRoleStructure(casting);
            count++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        wolf.superType(creature);
        creature.playsRole(monster);

        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertTrue(ValidateGlobalRules.validatePlaysRoleStructure(casting));
        }

        mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), werewolf.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next().remove();
        ((Edge) mindmapsGraph.getTinkerPopGraph().traversal().V(wolf.getBaseIdentifier()).outE(DataType.EdgeLabel.AKO.getLabel()).as("edge").otherV().hasId(creature.getBaseIdentifier()).select("edge").next()).remove();
        ((Edge) mindmapsGraph.getTinkerPopGraph().traversal().V(creature.getBaseIdentifier()).outE(DataType.EdgeLabel.PLAYS_ROLE.getLabel()).as("edge").otherV().hasId(monster.getBaseIdentifier()).select("edge").next()).remove();

        werewolf.type(wolf);
        wolf.type(creature);
        creature.type(wolf);

        flags = new boolean[]{false, false};
        count = 0;
        for (CastingImpl casting : assertion.getMappingCasting()) {
            flags[count] = ValidateGlobalRules.validatePlaysRoleStructure(casting);
            count ++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), werewolf.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next().remove();
        mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), wolf.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next().remove();
        mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), creature.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next().remove();

        werewolf.type(wolf);
        wolf.playsRole(animal);
        wolf.superType(creature);
        creature.playsRole(monster);

        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertTrue(ValidateGlobalRules.validatePlaysRoleStructure(casting));
        }

        ((Edge) mindmapsGraph.getTinkerPopGraph().traversal().V(creature.getBaseIdentifier()).outE(DataType.EdgeLabel.PLAYS_ROLE.getLabel()).as("edge").otherV().hasId(monster.getBaseIdentifier()).select("edge").next()).remove();
        ((Edge) mindmapsGraph.getTinkerPopGraph().traversal().V(hunter.getBaseIdentifier()).outE(DataType.EdgeLabel.PLAYS_ROLE.getLabel()).as("edge").otherV().hasId(witcher.getBaseIdentifier()).select("edge").next()).remove();

        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertFalse(ValidateGlobalRules.validatePlaysRoleStructure(casting));
        }
    }

    @Test
    public void testValidateHasSingleIncomingHasRoleEdge() throws Exception {
        RoleType hunter = mindmapsGraph.putRoleType("hunter");
        RoleType monster = mindmapsGraph.putRoleType("monster");
        RelationType kills = mindmapsGraph.putRelationType("kills");
        RelationType kills2 = mindmapsGraph.putRelationType("kills2");

        assertFalse(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(hunter));
        assertFalse(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(monster));

        kills.hasRole(hunter);
        kills2.hasRole(hunter);

        assertFalse(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(hunter));

        kills2.deleteHasRole(hunter);
        kills.hasRole(monster);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(hunter));
        assertTrue(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(monster));
    }

    @Test
    public void testValidateRelationTypeHasRoles() throws Exception {
        RoleType hunter = mindmapsGraph.putRoleType("hunter");
        RoleType monster = mindmapsGraph.putRoleType("monster");
        RoleType creature = mindmapsGraph.putRoleType("creature");
        RelationType kills = mindmapsGraph.putRelationType("kills");

        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills));
        kills.hasRole(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills));
        kills.hasRole(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills));
        kills.hasRole(monster);
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills));
        kills.hasRole(creature);
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills));
    }

    @Test
    public void testValidateAssertionStructure() throws Exception {
        EntityType fakeType = mindmapsGraph.putEntityType("Fake Concept");
        RoleType napper = mindmapsGraph.putRoleType("napper");
        RoleType hunter = mindmapsGraph.putRoleType("hunter");
        RoleType monster = mindmapsGraph.putRoleType("monster");
        RoleType creature = mindmapsGraph.putRoleType("creature");
        Instance cathulu = mindmapsGraph.putEntity("cathulu", fakeType);
        Instance werewolf = mindmapsGraph.putEntity("werewolf", fakeType);
        Instance cartman = mindmapsGraph.putEntity("cartman", fakeType);
        RelationType kills = mindmapsGraph.putRelationType("kills");
        RelationType naps = mindmapsGraph.putRelationType("naps").hasRole(napper);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), kills).
                putRolePlayer(hunter, cartman).putRolePlayer(monster, werewolf).putRolePlayer(creature, cathulu);

        kills.hasRole(monster);
        assertFalse(ValidateGlobalRules.validateRelationshipStructure(assertion));

        kills.hasRole(hunter);
        kills.hasRole(creature);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion));

        RelationImpl assertion2 = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), naps).putRolePlayer(hunter, cathulu);
        assertFalse(ValidateGlobalRules.validateRelationshipStructure(assertion2));
    }


    @Test
    public void testAbstractConceptValidation(){
        RoleType roleType = mindmapsGraph.putRoleType("hasRole");
        RelationType relationType = mindmapsGraph.putRelationType("relationType");

        assertFalse(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType));
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationType));

        roleType.setAbstract(true);
        relationType.setAbstract(true);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType));
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationType));
    }

    @Test
    public void testAbstractInstancesDoNotValidateSubTypes(){
        RoleType r1 = mindmapsGraph.putRoleType("r1");
        RoleType r2 = mindmapsGraph.putRoleType("r2");
        EntityType entityType = mindmapsGraph.putEntityType("entityType").playsRole(r1).playsRole(r2);
        RelationType relationType = mindmapsGraph.putRelationType("relationType").setAbstract(true);
        RelationType hasCast = mindmapsGraph.putRelationType("has cast").superType(relationType).hasRole(r1).hasRole(r2);

        Entity e1 = mindmapsGraph.putEntity("e1", entityType);
        Entity e2 = mindmapsGraph.putEntity("e2", entityType);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), hasCast).putRolePlayer(r1, e1).putRolePlayer(r2, e2);

        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges((TypeImpl) relationType));
    }

    @Test
    public void validateIsAbstractHasNoIncomingIsaEdges() {
        TypeImpl x1 = (TypeImpl) mindmapsGraph.putEntityType("x1");
        TypeImpl x2 = (TypeImpl) mindmapsGraph.putEntityType("x2'");
        TypeImpl x3 = (TypeImpl) mindmapsGraph.putEntityType("x3");
        TypeImpl x4 = (TypeImpl) mindmapsGraph.putEntityType("x4'");

        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1));
        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x2));
        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x3));
        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x4));

        x2.superType(x1);

        assertTrue(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1));

        x3.type(x1);

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1));
    }
}
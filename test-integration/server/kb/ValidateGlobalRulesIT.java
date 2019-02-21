/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.kb;

import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.kb.concept.EntityImpl;
import grakn.core.server.kb.concept.EntityTypeImpl;
import grakn.core.server.kb.concept.RelationImpl;
import grakn.core.server.kb.concept.RoleImpl;
import grakn.core.server.kb.concept.ThingImpl;
import grakn.core.server.kb.structure.Casting;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void testValidatePlaysStructure() {
        EntityTypeImpl wolf = (EntityTypeImpl) tx.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) tx.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) tx.putEntityType("hunter");
        RelationType hunts = tx.putRelationType("hunts");
        RoleImpl witcher = (RoleImpl) tx.putRole("witcher");
        RoleImpl monster = (RoleImpl) tx.putRole("monster");
        Thing geralt = hunter.create();
        ThingImpl werewolf = (ThingImpl) wolf.create();

        RelationImpl assertion = (RelationImpl) hunts.create().
                assign(witcher, geralt).assign(monster, werewolf);
        assertion.reified().get().castingsRelation().forEach(rolePlayer ->
                assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty()));

        hunter.plays(witcher);

        boolean [] flags = {false, false};
        int count = 0;

        for (Casting casting : assertion.reified().get().castingsRelation().collect(Collectors.toSet())) {
            flags[count] = !ValidateGlobalRules.validatePlaysAndRelatesStructure(casting).isEmpty();
            count++;
        }
        assertTrue(flags[0] && flags[1]);

        wolf.sup(creature);
        creature.plays(monster);

        for (Casting casting : assertion.reified().get().castingsRelation().collect(Collectors.toSet())) {
            assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(casting).isEmpty());
        }
    }

    @Test
    public void testValidatePlaysStructureUnique() {
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        RelationType relationshipType = tx.putRelationType("rt").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("et");

        ((EntityTypeImpl) entityType).play(role1, true);
        ((EntityTypeImpl) entityType).play(role2, false);

        Entity other1 = entityType.create();
        Entity other2 = entityType.create();

        EntityImpl entity = (EntityImpl) entityType.create();

        RelationImpl relation1 = (RelationImpl) relationshipType.create()
                .assign(role2, other1).assign(role1, entity);

        // Valid with only a single relation
        relation1.reified().get().castingsRelation().forEach(rolePlayer ->
                assertTrue(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty()));

        RelationImpl relation2 = (RelationImpl) relationshipType.create()
                .assign(role2, other2).assign(role1, entity);

        // Invalid with multiple relations
        relation1.reified().get().castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRole().equals(role1)) {
                assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty());
            }
        });
        relation2.reified().get().castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRole().equals(role1)) {
                assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty());
            }
        });
    }

    @Test
    public void testValidateRelationTypeRelates() {
        Role hunter = tx.putRole("hunter");
        RelationType kills = tx.putRelationType("kills");

        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.relates(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
    }


    @Test
    public void testAbstractConceptValidation(){
        Role role = tx.putRole("relates");
        RelationType relationshipType = tx.putRelationType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationshipType).isPresent());

        relationshipType.isAbstract(true);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationshipType).isPresent());
    }
}
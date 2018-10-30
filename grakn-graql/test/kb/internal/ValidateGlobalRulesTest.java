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

package ai.grakn.kb.internal;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.concept.EntityImpl;
import ai.grakn.kb.internal.concept.EntityTypeImpl;
import ai.grakn.kb.internal.concept.RelationshipImpl;
import ai.grakn.kb.internal.concept.RoleImpl;
import ai.grakn.kb.internal.concept.ThingImpl;
import ai.grakn.kb.internal.structure.Casting;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesTest extends TxTestBase {

    @Test
    public void testValidatePlaysStructure() throws Exception {
        EntityTypeImpl wolf = (EntityTypeImpl) tx.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) tx.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) tx.putEntityType("hunter");
        RelationshipType hunts = tx.putRelationshipType("hunts");
        RoleImpl witcher = (RoleImpl) tx.putRole("witcher");
        RoleImpl monster = (RoleImpl) tx.putRole("monster");
        Thing geralt = hunter.create();
        ThingImpl werewolf = (ThingImpl) wolf.create();

        RelationshipImpl assertion = (RelationshipImpl) hunts.create().
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
        RelationshipType relationshipType = tx.putRelationshipType("rt").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("et");

        ((EntityTypeImpl) entityType).play(role1, true);
        ((EntityTypeImpl) entityType).play(role2, false);

        Entity other1 = entityType.create();
        Entity other2 = entityType.create();

        EntityImpl entity = (EntityImpl) entityType.create();

        RelationshipImpl relation1 = (RelationshipImpl) relationshipType.create()
                .assign(role2, other1).assign(role1, entity);

        // Valid with only a single relation
        relation1.reified().get().castingsRelation().forEach(rolePlayer ->
                assertTrue(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty()));

        RelationshipImpl relation2 = (RelationshipImpl) relationshipType.create()
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
    public void testValidateRelationTypeRelates() throws Exception {
        Role hunter = tx.putRole("hunter");
        RelationshipType kills = tx.putRelationshipType("kills");

        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.relates(hunter);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
    }


    @Test
    public void testAbstractConceptValidation(){
        Role role = tx.putRole("relates");
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationshipType).isPresent());

        relationshipType.isAbstract(true);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationshipType).isPresent());
    }
}
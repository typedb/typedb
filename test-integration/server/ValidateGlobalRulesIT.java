/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.server;

import grakn.core.concept.impl.RelationImpl;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import grakn.core.util.ConceptDowncasting;
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
        tx = session.writeTransaction();
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void testValidatePlaysStructure() {
        EntityType wolf = tx.putEntityType("wolf");
        EntityType creature = tx.putEntityType("creature");
        EntityType hunter = tx.putEntityType("hunter");
        RelationType hunts = tx.putRelationType("hunts");
        Role witcher = tx.putRole("witcher");
        Role monster = tx.putRole("monster");
        Thing geralt = hunter.create();
        Thing werewolf = wolf.create();

        RelationImpl assertion = ConceptDowncasting.relation(hunts.create().
                assign(witcher, geralt).assign(monster, werewolf));
        assertion.reified().castingsRelation().forEach(rolePlayer ->
                assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty()));

        hunter.plays(witcher);

        boolean [] flags = {false, false};
        int count = 0;

        for (Casting casting : assertion.reified().castingsRelation().collect(Collectors.toSet())) {
            flags[count] = !ValidateGlobalRules.validatePlaysAndRelatesStructure(casting).isEmpty();
            count++;
        }
        assertTrue(flags[0] && flags[1]);

        wolf.sup(creature);
        creature.plays(monster);

        for (Casting casting : assertion.reified().castingsRelation().collect(Collectors.toSet())) {
            assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(casting).isEmpty());
        }
    }

    @Test
    public void testValidatePlaysStructureUnique() {
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        RelationType relationType = tx.putRelationType("rt").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("et");

        entityType.play(role1, true);
        entityType.play(role2, false);

        Entity other1 = entityType.create();
        Entity other2 = entityType.create();

        Entity entity = entityType.create();

        RelationImpl relation1 = ConceptDowncasting.relation(relationType.create()
                .assign(role2, other1).assign(role1, entity));

        // Valid with only a single relation
        relation1.reified().castingsRelation().forEach(rolePlayer ->
                assertTrue(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty()));

        RelationImpl relation2 = ConceptDowncasting.relation(relationType.create()
                .assign(role2, other2).assign(role1, entity));

        // Invalid with multiple relations
        relation1.reified().castingsRelation().forEach(rolePlayer -> {
            if (rolePlayer.getRole().equals(role1)) {
                assertFalse(ValidateGlobalRules.validatePlaysAndRelatesStructure(rolePlayer).isEmpty());
            }
        });
        relation2.reified().castingsRelation().forEach(rolePlayer -> {
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
        RelationType relationType = tx.putRelationType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());

        relationType.isAbstract(true);

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());
    }
}
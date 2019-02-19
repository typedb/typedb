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

package grakn.core.server.kb.structure;

import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.kb.concept.RelationImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CastingIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private Transaction tx;
    private Session session;

    private RelationType relationshipType;
    private EntityType entityType;
    private Role role3;
    private Role role2;
    private Role role1;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
        role1 = tx.putRole("role1");
        role2 = tx.putRole("role2");
        role3 = tx.putRole("role3");
        entityType = tx.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);
        relationshipType = tx.putRelationshipType("Relationship Type").relates(role1).relates(role2).relates(role3);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }
    @Test
    public void whenCreatingRelation_EnsureRolePlayerContainsInstanceRoleTypeRelationTypeAndRelation(){
        Entity e1 = entityType.create();

        RelationImpl relation = (RelationImpl) relationshipType.create().
                assign(role1, e1);

        Set<Casting> castings = relation.reified().get().castingsRelation().collect(Collectors.toSet());

        castings.forEach(rolePlayer -> {
            assertEquals(e1, rolePlayer.getRolePlayer());
            assertEquals(role1, rolePlayer.getRole());
            assertEquals(relationshipType, rolePlayer.getRelationshipType());
            assertEquals(relation, rolePlayer.getRelationship());
        });
    }

    @Test
    public void whenUpdatingRelation_EnsureRolePlayersAreUpdated(){
        Entity e1 = entityType.create();
        Entity e3 = entityType.create();

        RelationImpl relation = (RelationImpl) relationshipType.create().
                assign(role1, e1);

        Set<Thing> things = relation.reified().get().castingsRelation().map(Casting::getRolePlayer).collect(Collectors.toSet());
        Set<Role> roles = relation.reified().get().castingsRelation().map(Casting::getRole).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1));
        assertThat(roles, containsInAnyOrder(role1));

        //Now Update
        relation.assign(role2, e1).assign(role3, e3);

        things = relation.reified().get().castingsRelation().map(Casting::getRolePlayer).collect(Collectors.toSet());
        roles = relation.reified().get().castingsRelation().map(Casting::getRole).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1, e3));
        assertThat(roles, containsInAnyOrder(role1, role2, role3));
    }
}
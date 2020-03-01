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
 */

package grakn.core.concept.structure;

import grakn.core.concept.impl.RelationImpl;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
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

    private RelationType relationType;
    private EntityType entityType;
    private Role role3;
    private Role role2;
    private Role role1;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
        role1 = tx.putRole("role1");
        role2 = tx.putRole("role2");
        role3 = tx.putRole("role3");
        entityType = tx.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);
        relationType = tx.putRelationType("Relation Type").relates(role1).relates(role2).relates(role3);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }
    @Test
    public void whenCreatingRelation_EnsureRolePlayerContainsInstanceRoleTypeRelationTypeAndRelation(){
        Entity e1 = entityType.create();

        Relation relation = relationType.create().
                assign(role1, e1);

        Set<Casting> castings = ConceptDowncasting.relation(relation).reified().castingsRelation().collect(Collectors.toSet());

        castings.forEach(rolePlayer -> {
            assertEquals(e1, rolePlayer.getRolePlayer());
            assertEquals(role1, rolePlayer.getRole());
            assertEquals(relationType, rolePlayer.getRelationType());
            assertEquals(relation, rolePlayer.getRelation());
        });
    }

    @Test
    public void whenUpdatingRelation_EnsureRolePlayersAreUpdated(){
        Entity e1 = entityType.create();
        Entity e3 = entityType.create();

        RelationImpl relation = ConceptDowncasting.relation(relationType.create().
                assign(role1, e1));

        Set<Thing> things = relation.reified().castingsRelation().map(Casting::getRolePlayer).collect(Collectors.toSet());
        Set<Role> roles = relation.reified().castingsRelation().map(Casting::getRole).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1));
        assertThat(roles, containsInAnyOrder(role1));

        //Now Update
        relation.assign(role2, e1).assign(role3, e3);

        things = relation.reified().castingsRelation().map(Casting::getRolePlayer).collect(Collectors.toSet());
        roles = relation.reified().castingsRelation().map(Casting::getRole).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1, e3));
        assertThat(roles, containsInAnyOrder(role1, role2, role3));
    }
}
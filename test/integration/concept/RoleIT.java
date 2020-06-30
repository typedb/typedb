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

package grakn.core.concept;

import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.test.rule.GraknTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RoleIT {
    private Role role;
    private RelationType relationType;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
        relationType = tx.putRelationType("RelationType").relates("My Role");
        role = relationType.role("My Role");
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenGettingTheRelationTypesARoleIsInvolvedIn_ReturnTheRelationTypes() {
        assertThat(role.relations().collect(toSet()), containsInAnyOrder(relationType));
    }

    @Test
    public void whenGettingTypeEntityTypesAllowedToPlayARole_ReturnTheEntityTypes(){
        Type type1 = tx.putEntityType("CT1").plays(role);
        Type type2 = tx.putEntityType("CT2").plays(role);
        Type type3 = tx.putEntityType("CT3").plays(role);
        Type type4 = tx.putEntityType("CT4").plays(role);
        assertThat(role.players().collect(toSet()), containsInAnyOrder(type1, type2, type3, type4));
    }

    @Test
    public void whenDeletingRoleTypeWithTypesWhichCanPlayIt_Throw(){
        // TODO this should really throw too, if the following test throws?
        relationType.relates("new-role");
        Role newRole = relationType.role("new-role");
        assertNotNull(newRole);
        newRole.delete();
        assertNull(tx.getRole("new-role", "RelationType"));

        Role role = tx.getRole("My Role", "RelationType");
        EntityType player = tx.putEntityType("player").plays(role);
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(role).getMessage());
        role.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        RelationType relation = tx.putRelationType("Thing").relates("New Role Type");
        Role role2 = relation.role("New Role Type");

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(role2).getMessage());

        role2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        RelationType relationType = tx.putRelationType("relationTypes").relates("roleA").relates("roleB");
        Role roleA = relationType.role("roleA");
        Role roleB = relationType.role("roleB");
        EntityType entityType = tx.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.create();
        Entity b = entityType.create();

        relationType.create().assign(roleA, a).assign(roleB, b);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(roleA).getMessage());

        roleA.delete();
    }


    /**
     * TODO put a test for role inheritance
     */

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws InvalidKBException {
        relationType.relates("roleA");
        Role roleA = relationType.role("roleA");
        RelationType relationType2 = tx.putRelationType("relationType2").relates("roleB");
        Role roleB = relationType2.role("roleB");
        tx.commit();

        assertThat(roleA.relations().collect(toSet()), containsInAnyOrder(relationType));
        assertThat(roleB.relations().collect(toSet()), containsInAnyOrder(relationType2));
        assertThat(role.relations().collect(toSet()), containsInAnyOrder(relationType, relationType2));
    }
}
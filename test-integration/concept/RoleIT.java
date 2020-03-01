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
import grakn.core.rule.GraknTestServer;
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
        tx = session.writeTransaction();
        role = tx.putRole("My Role");
        relationType = tx.putRelationType("RelationType");
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenGettingTheRelationTypesARoleIsInvolvedIn_ReturnTheRelationTypes() {
        assertThat(role.relations().collect(toSet()), empty());
        relationType.relates(role);
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
        Role foundType = tx.getRole("My Role");
        assertNotNull(foundType);
        foundType.delete();
        assertNull(tx.getRole("My Role"));

        Role role = tx.putRole("New Role Type");
        tx.putEntityType("Entity Type").plays(role);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(role).getMessage());

        role.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        Role role2 = tx.putRole("New Role Type");
        tx.putRelationType("Thing").relates(role2).relates(role);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(role2).getMessage());

        role2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        RelationType relationType = tx.putRelationType("relationTypes").relates(roleA).relates(roleB);
        EntityType entityType = tx.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.create();
        Entity b = entityType.create();

        relationType.create().assign(roleA, a).assign(roleB, b);

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotBeDeleted(roleA).getMessage());

        roleA.delete();
    }

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws InvalidKBException {
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        relationType.relates(roleA).relates(role);
        RelationType relationType2 = tx.putRelationType("relationType2").relates(roleB).relates(role);
        tx.commit();

        assertThat(roleA.relations().collect(toSet()), containsInAnyOrder(relationType));
        assertThat(roleB.relations().collect(toSet()), containsInAnyOrder(relationType2));
        assertThat(role.relations().collect(toSet()), containsInAnyOrder(relationType, relationType2));
    }
}
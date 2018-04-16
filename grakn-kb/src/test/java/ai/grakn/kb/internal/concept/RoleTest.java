/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.kb.internal.concept;

/*-
 * #%L
 * grakn-kb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.internal.TxTestBase;
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RoleTest extends TxTestBase {
    private Role role;
    private RelationshipType relationshipType;

    @Before
    public void setup(){
        role = tx.putRole("My Role");
        relationshipType = tx.putRelationshipType("RelationshipType");
    }

    @Test
    public void whenGettingTheRelationTypesARoleIsInvolvedIn_ReturnTheRelationTypes() throws Exception {
        assertThat(role.relationshipTypes().collect(toSet()), empty());
        relationshipType.relates(role);
        assertThat(role.relationshipTypes().collect(toSet()), containsInAnyOrder(relationshipType));
    }

    @Test
    public void whenGettingTypeEntityTypesAllowedToPlayARole_ReturnTheEntityTypes(){
        Type type1 = tx.putEntityType("CT1").plays(role);
        Type type2 = tx.putEntityType("CT2").plays(role);
        Type type3 = tx.putEntityType("CT3").plays(role);
        Type type4 = tx.putEntityType("CT4").plays(role);
        assertThat(role.playedByTypes().collect(toSet()), containsInAnyOrder(type1, type2, type3, type4));
    }

    @Test
    public void whenDeletingRoleTypeWithTypesWhichCanPlayIt_Throw(){
        Role foundType = tx.getRole("My Role");
        assertNotNull(foundType);
        foundType.delete();
        assertNull(tx.getRole("My Role"));

        Role role = tx.putRole("New Role Type");
        tx.putEntityType("Entity Type").plays(role);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(role).getMessage());

        role.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        Role role2 = tx.putRole("New Role Type");
        tx.putRelationshipType("Thing").relates(role2).relates(role);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(role2).getMessage());

        role2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes").relates(roleA).relates(roleB);
        EntityType entityType = tx.putEntityType("entityType").plays(roleA).plays(roleB);

        Entity a = entityType.addEntity();
        Entity b = entityType.addEntity();

        relationshipType.addRelationship().addRolePlayer(roleA, a).addRolePlayer(roleB, b);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(roleA).getMessage());

        roleA.delete();
    }

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws InvalidKBException {
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        relationshipType.relates(roleA).relates(role);
        RelationshipType relationshipType2 = tx.putRelationshipType("relationshipType2").relates(roleB).relates(role);
        tx.commit();

        assertThat(roleA.relationshipTypes().collect(toSet()), containsInAnyOrder(relationshipType));
        assertThat(roleB.relationshipTypes().collect(toSet()), containsInAnyOrder(relationshipType2));
        assertThat(role.relationshipTypes().collect(toSet()), containsInAnyOrder(relationshipType, relationshipType2));
    }
}

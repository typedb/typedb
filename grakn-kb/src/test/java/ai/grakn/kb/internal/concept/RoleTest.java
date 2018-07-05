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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.concept;

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
        assertThat(role.relationships().collect(toSet()), empty());
        relationshipType.relate(role);
        assertThat(role.relationships().collect(toSet()), containsInAnyOrder(relationshipType));
    }

    @Test
    public void whenGettingTypeEntityTypesAllowedToPlayARole_ReturnTheEntityTypes(){
        Type type1 = tx.putEntityType("CT1").play(role);
        Type type2 = tx.putEntityType("CT2").play(role);
        Type type3 = tx.putEntityType("CT3").play(role);
        Type type4 = tx.putEntityType("CT4").play(role);
        assertThat(role.players().collect(toSet()), containsInAnyOrder(type1, type2, type3, type4));
    }

    @Test
    public void whenDeletingRoleTypeWithTypesWhichCanPlayIt_Throw(){
        Role foundType = tx.getRole("My Role");
        assertNotNull(foundType);
        foundType.delete();
        assertNull(tx.getRole("My Role"));

        Role role = tx.putRole("New Role Type");
        tx.putEntityType("Entity Type").play(role);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(role).getMessage());

        role.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRelationTypes_Throw(){
        Role role2 = tx.putRole("New Role Type");
        tx.putRelationshipType("Thing").relate(role2).relate(role);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(role2).getMessage());

        role2.delete();
    }

    @Test
    public void whenDeletingRoleTypeWithRolePlayers_Throw(){
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes").relate(roleA).relate(roleB);
        EntityType entityType = tx.putEntityType("entityType").play(roleA).play(roleB);

        Entity a = entityType.create();
        Entity b = entityType.create();

        relationshipType.create().assign(roleA, a).assign(roleB, b);

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotBeDeleted(roleA).getMessage());

        roleA.delete();
    }

    @Test
    public void whenAddingRoleTypeToMultipleRelationTypes_EnsureItLinkedToBothRelationTypes() throws InvalidKBException {
        Role roleA = tx.putRole("roleA");
        Role roleB = tx.putRole("roleB");
        relationshipType.relate(roleA).relate(role);
        RelationshipType relationshipType2 = tx.putRelationshipType("relationshipType2").relate(roleB).relate(role);
        tx.commit();

        assertThat(roleA.relationships().collect(toSet()), containsInAnyOrder(relationshipType));
        assertThat(roleB.relationships().collect(toSet()), containsInAnyOrder(relationshipType2));
        assertThat(role.relationships().collect(toSet()), containsInAnyOrder(relationshipType, relationshipType2));
    }
}
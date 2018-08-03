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

package ai.grakn.test.property.kb;

import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.ArrayUtils.addAll;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(JUnitQuickcheck.class)
public class RelationshipPropertyTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Property
    public void whenAddingARolePlayer_ItIsAddedToTheCollectionOfRolePlayers(
            Relationship relationship, @NonMeta @FromTx Role role, @FromTx Thing rolePlayer) {

        relationship.assign(role, rolePlayer);

        assertThat(relationship.rolePlayers().collect(toSet()), hasItem(rolePlayer));
    }

    @Property(onMinimalCounterexample = GraknTxs.class)
    public void whenAddingARolePlayerPlayingARole_TheRolePlayerIsAddedToTheCollectionOfRolePlayersForThatRole(
            Relationship relationship, @NonMeta @FromTx Role role, @FromTx Thing rolePlayer) {

        relationship.assign(role, rolePlayer);

        assertThat(relationship.rolePlayers(role).collect(toSet()), hasItem(rolePlayer));
    }

    @Property
    public void whenAddingARolePlayer_NoRolePlayersAreRemoved(
            Relationship relationship, @NonMeta @FromTx Role role, @FromTx Thing rolePlayer) {

        Thing[] rolePlayers = relationship.rolePlayers(role).toArray(Thing[]::new);

        relationship.assign(role, rolePlayer);

        assertThat(relationship.rolePlayers(role).collect(toSet()), hasItems(rolePlayers));
    }

    @Property
    public void whenCallingRolePlayers_TheResultIsASet(Relationship relationship, @FromTx Role[] roles) {
        Collection<Thing> rolePlayers = relationship.rolePlayers(roles).collect(toSet());
        Set<Thing> rolePlayersSet = newHashSet(rolePlayers);
        assertEquals(rolePlayers.size(), rolePlayersSet.size());
    }

    @Property
    public void whenCallingRolePlayersWithoutArgs_ReturnRolePlayersOfAllRoleTypes(Relationship relationship) {
        Role[] allRoles = new Role[relationship.rolePlayersMap().size()];
        relationship.rolePlayersMap().keySet().toArray(allRoles);

        assertEquals(relationship.rolePlayers().collect(toSet()), relationship.rolePlayers(allRoles).collect(toSet()));
    }

    @Property
    public void whenCallingRolePlayersWithXandY_IsTheSameAsCallingRolePlayersXAndRolePlayersY(
            Relationship relationship, @FromTx Role[] rolesX, @FromTx Role[] rolesY) {

        Role[] rolesXY = (Role[]) addAll(rolesX, rolesY);

        Set<Thing> expected =
                union(relationship.rolePlayers(rolesX).collect(toSet()), relationship.rolePlayers(rolesY).collect(toSet()));

        assertEquals(expected, relationship.rolePlayers(rolesXY).collect(toSet()));
    }
}
/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.test.property;

import ai.grakn.concept.Relation;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static org.apache.commons.lang.ArrayUtils.addAll;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(JUnitQuickcheck.class)
public class RelationPropertyTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Property
    public void whenAddingARolePlayer_ItIsAddedToTheCollectionOfRolePlayers(
            Relation relation, @NonMeta @FromGraph Role role, @FromGraph Thing rolePlayer) {

        relation.addRolePlayer(role, rolePlayer);

        assertThat(relation.rolePlayers(), hasItem(rolePlayer));
    }

    @Property(onMinimalCounterexample = GraknGraphs.class)
    public void whenAddingARolePlayerPlayingARole_TheRolePlayerIsAddedToTheCollectionOfRolePlayersForThatRole(
            Relation relation, @NonMeta @FromGraph Role role, @FromGraph Thing rolePlayer) {

        relation.addRolePlayer(role, rolePlayer);

        assertThat(relation.rolePlayers(role), hasItem(rolePlayer));
    }

    @Property
    public void whenAddingARolePlayer_NoRolePlayersAreRemoved(
            Relation relation, @NonMeta @FromGraph Role role, @FromGraph Thing rolePlayer) {

        Thing[] rolePlayers = relation.rolePlayers(role).toArray(new Thing[0]);

        relation.addRolePlayer(role, rolePlayer);

        assertThat(relation.rolePlayers(role), hasItems(rolePlayers));
    }

    @Property
    public void whenCallingRolePlayers_TheResultIsASet(Relation relation, @FromGraph Role[] roles) {
        Collection<Thing> rolePlayers = relation.rolePlayers(roles);
        Set<Thing> rolePlayersSet = newHashSet(rolePlayers);
        assertEquals(rolePlayers.size(), rolePlayersSet.size());
    }

    @Property
    public void whenCallingRolePlayersWithoutArgs_ReturnRolePlayersOfAllRoleTypes(Relation relation) {
        Role[] allRoles = new Role[relation.allRolePlayers().size()];
        relation.allRolePlayers().keySet().toArray(allRoles);

        assertEquals(relation.rolePlayers(), relation.rolePlayers(allRoles));
    }

    @Property
    public void whenCallingRolePlayersWithXandY_IsTheSameAsCallingRolePlayersXAndRolePlayersY(
            Relation relation, @FromGraph Role[] rolesX, @FromGraph Role[] rolesY) {

        Role[] rolesXY = (Role[]) addAll(rolesX, rolesY);

        Set<Thing> expected =
                union(newHashSet(relation.rolePlayers(rolesX)), newHashSet(relation.rolePlayers(rolesY)));

        assertEquals(expected, newHashSet(relation.rolePlayers(rolesXY)));
    }
}

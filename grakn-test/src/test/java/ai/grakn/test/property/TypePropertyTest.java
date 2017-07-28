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

import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.Meta;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.test.property.PropertyUtil.choose;
import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnitQuickcheck.class)
public class TypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenSettingAMetaTypeAsAbstract_Throw(@Meta Type type, boolean isAbstract) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.setAbstract(isAbstract);
    }

    @Property
    public void whenMakingAMetaTypePlayRole_Throw(@Meta Type type, Role role) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.plays(role);
    }

    @Property
    public void whenGivingAMetaTypeAKey_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.key(resourceType);
    }

    @Property
    public void whenGivingAMetaTypeAResource_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.resource(resourceType);
    }

    @Ignore // TODO: Fails very rarely and only remotely
    @Property
    public void whenDeletingATypeWithIndirectInstances_Throw(@NonMeta Type type) {
        assumeThat(type.instances(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotBeDeleted(type).getMessage());
        type.delete();
    }

    @Ignore // TODO: Fix this (Bug #16191)
    @Property
    public void whenATypeWithDirectInstancesIsSetToAbstract_Throw(Type type) {
        assumeThat(PropertyUtil.directInstances(type), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(IS_ABSTRACT.getMessage(type.getLabel()));

        type.setAbstract(true);
    }

    @Property
    public void whenSettingATypeAbstractFlag_TheTypesAbstractFlagIsSet(@NonMeta Type type, boolean isAbstract) {
        assumeThat(PropertyUtil.directInstances(type), empty());

        type.setAbstract(isAbstract);
        assertEquals(isAbstract, type.isAbstract());
    }

    @Property
    public void whenGettingIndirectInstances_ReturnDirectInstancesAndIndirectInstancesOfDirectSubTypes(Type type) {
        Collection<Type> directSubTypes = PropertyUtil.directSubs(type);
        Thing[] expected = Stream.concat(
            PropertyUtil.directInstances(type).stream(),
            directSubTypes.stream().flatMap(subType -> subType.instances().stream())
        ).toArray(Thing[]::new);

        assertThat(type.instances(), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingPlays_ResultIsASupersetOfDirectSuperTypePlays(Type type) {
        assumeNotNull(type.sup());
        assertTrue(type.plays().containsAll(type.sup().plays()));
    }

    @Property
    public void ATypePlayingARoleIsEquivalentToARoleBeingPlayed(Type type, @FromGraph Role role) {
        assertEquals(type.plays().contains(role), role.playedByTypes().contains(type));
    }

    @Property
    public void whenAddingAPlays_TheTypePlaysThatRoleAndNoOtherNewRoles(
            @NonMeta Type type, @FromGraph Role role) {
        Set<Role> previousPlays = Sets.newHashSet(type.plays());
        type.plays(role);
        Set<Role> newPlays = Sets.newHashSet(type.plays());

        assertEquals(newPlays, Sets.union(previousPlays, ImmutableSet.of(role)));
    }

    @Property
    public void whenAddingAPlaysToATypesIndirectSuperType_TheTypePlaysThatRole(
            Type type, @FromGraph Role role, long seed) {
        OntologyConcept superConcept = PropertyUtil.choose(PropertyUtil.indirectSupers(type), seed);
        assumeTrue(superConcept.isType());
        assumeFalse(isMetaLabel(superConcept.getLabel()));

        Type superType = superConcept.asType();

        Set<Role> previousPlays = Sets.newHashSet(type.plays());
        superType.plays(role);
        Set<Role> newPlays = Sets.newHashSet(type.plays());

        assertEquals(newPlays, Sets.union(previousPlays, ImmutableSet.of(role)));
    }

    @Property
    public void whenDeletingAPlaysAndTheDirectSuperTypeDoesNotPlaysThatRole_TheTypeNoLongerPlaysThatRole(
            @NonMeta Type type, @FromGraph Role role) {
        assumeThat(type.sup().plays(), not(hasItem(role)));
        type.deletePlays(role);
        assertThat(type.plays(), not(hasItem(role)));
    }

    @Property
    public void whenDeletingAPlaysAndTheDirectSuperTypePlaysThatRole_TheTypeStillPlaysThatRole(
            @NonMeta Type type, long seed) {
        Role role = PropertyUtil.choose(type.sup() + " plays no roles", type.sup().plays(), seed);
        type.deletePlays(role);
        assertThat(type.plays(), hasItem(role));
    }

    // TODO: Tests for `resource` and `key`
    // TODO: Tests for scope methods and inheritance
}

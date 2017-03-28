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
 *
 */

package ai.grakn.graph.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.generator.AbstractTypeGenerator.Meta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.generator.GraknGraphs.withImplicitConceptsVisible;
import static ai.grakn.util.Schema.MetaSchema.isMetaName;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class TypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenSettingAMetaTypeAsAbstract_Throw(@Meta Type type, boolean isAbstract) {
        // TODO: Better define exception
        exception.expect(Exception.class);
        type.setAbstract(isAbstract);
    }

    @Property
    public void whenMakingAMetaTypePlayRole_Throw(@Meta Type type, RoleType roleType) {
        // TODO: Better define exception
        exception.expect(Exception.class);
        type.playsRole(roleType);
    }

    @Property
    public void whenGivingAMetaTypeAKey_Throw(@Meta Type type, ResourceType resourceType) {
        // TODO: Better define exception
        exception.expect(Exception.class);
        type.key(resourceType);
    }

    @Property
    public void whenGivingAMetaTypeAResource_Throw(@Meta Type type, ResourceType resourceType) {
        // TODO: Better define exception
        exception.expect(Exception.class);
        type.resource(resourceType);
    }

    @Property
    public void whenDeletingATypeWithDirectSubTypes_Throw(Type type) {
        // TODO: Better define exception
        exception.expect(Exception.class);
        type.superType().delete();
    }

    @Property
    public void whenDeletingATypeWithIndirectInstances_Throw(Type type) {
        assumeThat(type.instances(), not(empty()));

        // TODO: Better define exception
        exception.expect(Exception.class);
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithHypothesisRules_Throw(Type type) {
        assumeThat(type.getRulesOfHypothesis(), not(empty()));

        // TODO: Better define exception
        exception.expect(Exception.class);
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithConclusionRules_Throw(Type type) {
        assumeThat(type.getRulesOfConclusion(), not(empty()));

        // TODO: Better define exception
        exception.expect(Exception.class);
        type.delete();
    }

    @Property
    public void whenCallingGetName_TheResultIsUnique(Type type1, @FromGraph Type type2) {
        assumeThat(type1, not(is(type2)));
        assertNotEquals(type1.getName(), type2.getName());
    }

    @Property
    public void whenCallingGetName_TheResultCanBeUsedToRetrieveTheSameType(
            @Open GraknGraph graph, @FromGraph Type type) {
        TypeName name = type.getName();
        assertEquals(type, graph.getType(name));
    }

    @Ignore // TODO: Make this pass!
    @Property
    public void whenATypeWithInstancesIsSetToAbstract_Throw(Type type) {
        assumeThat(type.instances(), not(empty()));

        // TODO: Better define exception
        exception.expect(Exception.class);
        type.setAbstract(true);
    }

    @Property
    public void whenSettingATypeAbstractFlag_TheTypesAbstractFlagIsSet(@Meta(false) Type type, boolean isAbstract) {
        type.setAbstract(isAbstract);
        assertEquals(isAbstract, type.isAbstract());
    }

    @Property
    public void whenATypeHasADirectSuperType_ItIsADirectSubTypeOfThatSuperType(
            @Open GraknGraph graph, @FromGraph Type subType) {
        Type superType = subType.superType();
        assertThat(directSubTypes(graph, superType), hasItem(subType));
    }

    @Property
    public void whenATypeHasAnIndirectSuperType_ItIsAnIndirectSubTypeOfThatSuperType(Type subType, long seed) {
        Type superType = choose(indirectSuperTypes(subType), seed);
        assertThat((Collection<Type>) superType.subTypes(), hasItem(subType));
    }

    @Property
    public void whenATypeHasAnIndirectSubType_ItIsAnIndirectSuperTypeOfThatSubType(Type superType, long seed) {
        Type subType = choose(superType.subTypes(), seed);
        assertThat(indirectSuperTypes(subType), hasItem(superType));
    }

    @Property
    public void whenGettingIndirectSubTypes_ReturnSelfAndIndirectSubTypesOfDirectSubTypes(
            @Open GraknGraph graph, @FromGraph Type type) {
        Collection<Type> directSubTypes = directSubTypes(graph, type);
        Type[] expected = Stream.concat(
                Stream.of(type),
                directSubTypes.stream().flatMap(subType -> subType.subTypes().stream())
        ).toArray(Type[]::new);

        assertThat(type.subTypes(), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingTheIndirectSubTypes_TheyContainTheType(Type type) {
        assertThat((Collection<Type>) type.subTypes(), hasItem(type));
    }

    @Property
    public void whenGettingTheIndirectSubTypesWithoutImplicitConceptsVisible_TheyDoNotContainImplicitConcepts(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(graph.implicitConceptsVisible());
        type.subTypes().forEach(subType -> {
            assertFalse(subType + " should not be implicit", subType.isImplicit());
        });
    }

    @Property
    public void whenGettingIndirectInstances_ReturnDirectInstancesAndIndirectInstancesOfDirectSubTypes(
            @Open GraknGraph graph, @FromGraph Type type) {
        Collection<Type> directSubTypes = directSubTypes(graph, type);
        Instance[] expected = Stream.concat(
            directInstances(type).stream(),
            directSubTypes.stream().flatMap(subType -> subType.instances().stream())
        ).toArray(Instance[]::new);

        assertThat(type.instances(), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingPlaysRoles_ResultIsASupersetOfDirectSuperTypePlaysRoles(Type type) {
        assumeNotNull(type.superType());
        assertTrue(type.playsRoles().containsAll(type.superType().playsRoles()));
    }

    @Property
    public void whenTypePlaysARole_ThatRoleIsPlayedByTheType(Type type, long seed) {
        RoleType roleType = choose(type.playsRoles(), seed);
        assertThat(roleType.playedByTypes(), hasItem(type));
    }

    @Property
    public void whenARoleIsPlayedByAType_TheTypePlaysThatRole(RoleType roleType, long seed) {
        Type type = choose(roleType.playedByTypes(), seed);
        assertThat(type.playsRoles(), hasItem(roleType));
    }

    @Property
    public void whenAddingAPlaysRole_TheTypePlaysThatRoleAndNoOtherNewRoles(
            @Meta(false) Type type, @FromGraph RoleType roleType) {
        assumeThat(type, not(is(roleType)));  // A role-type cannot play itself, TODO: is this sensible?

        Set<RoleType> previousPlaysRoles = Sets.newHashSet(type.playsRoles());
        type.playsRole(roleType);
        Set<RoleType> newPlaysRoles = Sets.newHashSet(type.playsRoles());

        assertEquals(newPlaysRoles, Sets.union(previousPlaysRoles, ImmutableSet.of(roleType)));
    }

    @Property
    public void whenAddingAPlaysRoleToATypesIndirectSuperType_TheTypePlaysThatRole(
            Type type, @FromGraph RoleType roleType, long seed) {
        Type superType = choose(indirectSuperTypes(type), seed);

        assumeFalse(isMetaName(superType.getName()));
        assumeThat(superType, not(is(roleType)));

        Set<RoleType> previousPlaysRoles = Sets.newHashSet(type.playsRoles());
        superType.playsRole(roleType);
        Set<RoleType> newPlaysRoles = Sets.newHashSet(type.playsRoles());

        assertEquals(newPlaysRoles, Sets.union(previousPlaysRoles, ImmutableSet.of(roleType)));
    }

    @Property
    public void whenDeletingAPlaysRoleAndTheDirectSuperTypeDoesNotPlaysThatRole_TheTypeNoLongerPlaysThatRole(
            @Meta(false) Type type, @FromGraph RoleType roleType) {
        assumeThat(type.superType().playsRoles(), not(hasItem(roleType)));
        type.deletePlaysRole(roleType);
        assertThat(type.playsRoles(), not(hasItem(roleType)));
    }

    @Property
    public void whenDeletingAPlaysRoleAndTheDirectSuperTypePlaysThatRole_TheTypeStillPlaysThatRole(
            @Meta(false) Type type, long seed) {
        RoleType roleType = choose(type.superType() + " plays no roles", type.superType().playsRoles(), seed);
        type.deletePlaysRole(roleType);
        assertThat(type.playsRoles(), hasItem(roleType));
    }

    // TODO: Tests for `resource` and `key`

    private Collection<Type> directSubTypes(GraknGraph graph, Type type) {
        return withImplicitConceptsVisible(graph, g ->
            type.subTypes().stream().filter(subType -> type.equals(subType.superType())).collect(toList())
        );
    }

    private Collection<Type> indirectSuperTypes(Type type) {
        Collection<Type> superTypes = Lists.newArrayList();

        do {
            superTypes.add(type);
            type = type.superType();
        } while (type != null);

        return superTypes;
    }

    private Collection<Instance> directInstances(Type type) {
        Collection<? extends Instance> indirectInstances = type.instances();
        return indirectInstances.stream().filter(instance -> type.equals(instance.type())).collect(toList());
    }

    private <T> T choose(Collection<? extends T> collection, long seed) {
        assumeThat(collection, not(empty()));
        return chooseWithoutCheck(collection, seed);
    }

    private <T> T choose(String message, Collection<? extends T> collection, long seed) {
        assumeThat(message, collection, not(empty()));
        return chooseWithoutCheck(collection, seed);
    }

    private <T> T chooseWithoutCheck(Collection<? extends T> collection, long seed) {
        int index = new Random(seed).nextInt(collection.size());
        Optional<? extends T> result = collection.stream().skip(index).findFirst();
        assert result.isPresent();
        return result.get();
    }
}

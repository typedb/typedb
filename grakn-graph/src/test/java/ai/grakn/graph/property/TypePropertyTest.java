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
import ai.grakn.exception.ConceptException;
import ai.grakn.generator.AbstractTypeGenerator.Meta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
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

import static ai.grakn.graph.property.PropertyUtil.choose;
import static ai.grakn.graph.property.PropertyUtil.directInstances;
import static ai.grakn.graph.property.PropertyUtil.directSubTypes;
import static ai.grakn.graph.property.PropertyUtil.indirectSuperTypes;
import static ai.grakn.util.ErrorMessage.CANNOT_DELETE;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.ErrorMessage.SUPER_TYPE_LOOP_DETECTED;
import static ai.grakn.util.Schema.MetaSchema.isMetaName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getName()));
        type.setAbstract(isAbstract);
    }

    @Property
    public void whenMakingAMetaTypePlayRole_Throw(@Meta Type type, RoleType roleType) {
        assumeThat(type, not(is(roleType)));

        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getName()));
        type.plays(roleType);
    }

    @Property
    public void whenGivingAMetaTypeAKey_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getName()));
        type.key(resourceType);
    }

    @Property
    public void whenGivingAMetaTypeAResource_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getName()));
        type.resource(resourceType);
    }

    @Property
    public void whenDeletingAMetaType_Throw(@Meta Type type) {
        exception.expect(ConceptException.class);
        exception.expectMessage(isOneOf(
                META_TYPE_IMMUTABLE.getMessage(type.getName()),
                CANNOT_DELETE.getMessage(type.getName())
        ));
        type.delete();
    }

    @Property
    public void whenDeletingATypeWithDirectSubTypes_Throw(@Meta(false) Type type) {
        Type superType = type.superType();
        assumeFalse(isMetaName(superType.getName()));

        exception.expect(ConceptException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(superType.getName()));
        superType.delete();
    }

    @Property
    public void whenDeletingATypeWithIndirectInstances_Throw(@Meta(false) Type type) {
        assumeThat(type.instances(), not(empty()));

        exception.expect(ConceptException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(type.getName()));
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithHypothesisRules_Throw(Type type) {
        assumeThat(type.getRulesOfHypothesis(), not(empty()));

        exception.expect(ConceptException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(type.getName()));
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithConclusionRules_Throw(Type type) {
        assumeThat(type.getRulesOfConclusion(), not(empty()));

        exception.expect(ConceptException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(type.getName()));
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
    public void whenATypeWithDirectInstancesIsSetToAbstract_Throw(Type type) {
        assumeThat(directInstances(type), not(empty()));

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
    public void whenGettingSuperType_TheResultIsNeverItself(Type type) {
        assertNotEquals(type, type.superType());
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
    public void whenSettingTheDirectSuperTypeOfAMetaType_Throw(
            @Meta Type subType, @FromGraph Type superType) {
        assumeTrue(sameType(subType, superType));

        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getName()));
        setDirectSuperType(subType, superType);
    }

    @Property
    public void whenSettingTheDirectSuperTypeToAnIndirectSubType_Throw(
            @Meta(false) Type type, long seed) {
        Type newSuperType = choose(type.subTypes(), seed);

        exception.expect(ConceptException.class);
        exception.expectMessage(SUPER_TYPE_LOOP_DETECTED.getMessage(type.getName(), newSuperType.getName()));
        setDirectSuperType(type, newSuperType);
    }

    @Property
    public void whenSettingTheDirectSuperType_TheDirectSuperTypeIsSet(
            @Meta(false) Type subType, @FromGraph Type superType) {
        assumeTrue(sameType(subType, superType));
        assumeThat((Collection<Type>) subType.subTypes(), not(hasItem(superType)));

        setDirectSuperType(subType, superType);

        assertEquals(superType, subType.superType());
    }

    @Property
    public void whenAddingADirectSubTypeThatIsAMetaType_Throw(
            Type superType, @Meta @FromGraph Type subType) {
        assumeTrue(sameType(subType, superType));

        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getName()));
        addDirectSubType(superType, subType);
    }

    @Property
    public void whenAddingADirectSubTypeWhichIsAnIndirectSuperType_Throw(
            @Meta(false) Type newSubType, long seed) {
        Type type = choose(newSubType.subTypes(), seed);

        exception.expect(ConceptException.class);
        exception.expectMessage(SUPER_TYPE_LOOP_DETECTED.getMessage(newSubType.getName(), type.getName()));
        addDirectSubType(type, newSubType);
    }

    @Property
    public void whenAddingADirectSubType_TheDirectSubTypeIsAdded(
            @Open GraknGraph graph, @FromGraph Type superType, @Meta(false) @FromGraph Type subType) {
        assumeTrue(sameType(subType, superType));
        assumeThat((Collection<Type>) subType.subTypes(), not(hasItem(superType)));

        addDirectSubType(superType, subType);

        assertThat(directSubTypes(graph, superType), hasItem(subType));
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
        assertTrue(type.plays().containsAll(type.superType().plays()));
    }

    @Property
    public void ATypePlayingARoleIsEquivalentToARoleBeingPlayed(Type type, @FromGraph RoleType roleType) {
        assertEquals(type.plays().contains(roleType), roleType.playedByTypes().contains(type));
    }

    @Property
    public void whenAddingAPlaysRole_TheTypePlaysThatRoleAndNoOtherNewRoles(
            @Meta(false) Type type, @FromGraph RoleType roleType) {
        assumeThat(type, not(is(roleType)));  // A role-type cannot play itself, TODO: is this sensible?

        Set<RoleType> previousPlaysRoles = Sets.newHashSet(type.plays());
        type.plays(roleType);
        Set<RoleType> newPlaysRoles = Sets.newHashSet(type.plays());

        assertEquals(newPlaysRoles, Sets.union(previousPlaysRoles, ImmutableSet.of(roleType)));
    }

    @Property
    public void whenAddingAPlaysRoleToATypesIndirectSuperType_TheTypePlaysThatRole(
            Type type, @FromGraph RoleType roleType, long seed) {
        Type superType = choose(indirectSuperTypes(type), seed);

        assumeFalse(isMetaName(superType.getName()));
        assumeThat(superType, not(is(roleType)));

        Set<RoleType> previousPlaysRoles = Sets.newHashSet(type.plays());
        superType.plays(roleType);
        Set<RoleType> newPlaysRoles = Sets.newHashSet(type.plays());

        assertEquals(newPlaysRoles, Sets.union(previousPlaysRoles, ImmutableSet.of(roleType)));
    }

    @Property
    public void whenDeletingAPlaysRoleAndTheDirectSuperTypeDoesNotPlaysThatRole_TheTypeNoLongerPlaysThatRole(
            @Meta(false) Type type, @FromGraph RoleType roleType) {
        assumeThat(type.superType().plays(), not(hasItem(roleType)));
        type.deletePlays(roleType);
        assertThat(type.plays(), not(hasItem(roleType)));
    }

    @Property
    public void whenDeletingAPlaysRoleAndTheDirectSuperTypePlaysThatRole_TheTypeStillPlaysThatRole(
            @Meta(false) Type type, long seed) {
        RoleType roleType = choose(type.superType() + " plays no roles", type.superType().plays(), seed);
        type.deletePlays(roleType);
        assertThat(type.plays(), hasItem(roleType));
    }

    // TODO: Tests for `resource` and `key`
    // TODO: Tests for scope methods and inheritance

    private boolean sameType(Type type1, Type type2) {
        return type1.isEntityType() && type2.isEntityType() ||
                type1.isRelationType() && type2.isRelationType() ||
                type1.isRoleType() && type2.isRoleType() ||
                type1.isResourceType() && type2.isResourceType() ||
                type1.isRuleType() && type2.isRuleType();
    }

    private void setDirectSuperType(Type subType, Type superType) {
        if (subType.isEntityType()) {
            subType.asEntityType().superType(superType.asEntityType());
        } else if (subType.isRelationType()) {
            subType.asRelationType().superType(superType.asRelationType());
        } else if (subType.isRoleType()) {
            subType.asRoleType().superType(superType.asRoleType());
        } else if (subType.isResourceType()) {
            subType.asResourceType().superType(superType.asResourceType());
        } else if (subType.isRuleType()) {
            subType.asRuleType().superType(superType.asRuleType());
        } else {
            fail("unreachable");
        }
    }

    private void addDirectSubType(Type superType, Type subType) {
        if (superType.isEntityType()) {
            superType.asEntityType().subType(subType.asEntityType());
        } else if (superType.isRelationType()) {
            superType.asRelationType().subType(subType.asRelationType());
        } else if (superType.isRoleType()) {
            superType.asRoleType().subType(subType.asRoleType());
        } else if (superType.isResourceType()) {
            superType.asResourceType().subType(subType.asResourceType());
        } else if (superType.isRuleType()) {
            superType.asRuleType().subType(subType.asRuleType());
        } else {
            fail("unreachable");
        }
    }
}

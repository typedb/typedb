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

package ai.grakn.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.Meta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.util.Schema;
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

import static ai.grakn.property.PropertyUtil.choose;
import static ai.grakn.property.PropertyUtil.directInstances;
import static ai.grakn.property.PropertyUtil.directSubs;
import static ai.grakn.property.PropertyUtil.indirectSuperTypes;
import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
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
        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getLabel()));
        type.setAbstract(isAbstract);
    }

    @Property
    public void whenMakingAMetaTypePlayRole_Throw(@Meta Type type, Role role) {
        assumeThat(type, not(is(role)));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getLabel()));
        type.plays(role);
    }

    @Property
    public void whenGivingAMetaTypeAKey_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getLabel()));
        type.key(resourceType);
    }

    @Property
    public void whenGivingAMetaTypeAResource_Throw(@Meta Type type, ResourceType resourceType) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getLabel()));
        type.resource(resourceType);
    }

    @Property
    public void whenDeletingAMetaType_Throw(@Meta Type type) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(isOneOf(
                META_TYPE_IMMUTABLE.getMessage(type.getLabel()),
                GraphOperationException.cannotBeDeleted(type).getMessage()
        ));
        type.delete();
    }

    @Property
    public void whenDeletingATypeWithDirectSubTypes_Throw(@Meta(false) Type type) {
        Type superType = type.sup();
        assumeFalse(isMetaLabel(superType.getLabel()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotBeDeleted(superType).getMessage());
        superType.delete();
    }

    @Ignore // TODO: Fails very rarely and only remotely
    @Property
    public void whenDeletingATypeWithIndirectInstances_Throw(@Meta(false) Type type) {
        assumeThat(type.instances(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotBeDeleted(type).getMessage());
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithHypothesisRules_Throw(Type type) {
        assumeThat(type.getRulesOfHypothesis(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotBeDeleted(type).getMessage());
        type.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingATypeWithConclusionRules_Throw(Type type) {
        assumeThat(type.getRulesOfConclusion(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotBeDeleted(type).getMessage());
        type.delete();
    }

    @Property
    public void whenCallingGetName_TheResultIsUnique(Type type1, @FromGraph Type type2) {
        assumeThat(type1, not(is(type2)));
        assertNotEquals(type1.getLabel(), type2.getLabel());
    }

    @Property
    public void whenCallingGetLabel_TheResultCanBeUsedToRetrieveTheSameType(
            @Open GraknGraph graph, @FromGraph Type type) {
        Label label = type.getLabel();
        assertEquals(type, graph.getOntologyConcept(label));
    }

    @Property
    public void whenATypeWithDirectInstancesIsSetToAbstract_Throw(Type type) {
        assumeThat(directInstances(type), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(IS_ABSTRACT.getMessage(type.getLabel()));

        type.setAbstract(true);
    }

    @Property
    public void whenSettingATypeAbstractFlag_TheTypesAbstractFlagIsSet(@Meta(false) Type type, boolean isAbstract) {
        assumeFalse(type.isRole()); //Temporary workaround since castings may die
        assumeThat(directInstances(type), empty());

        type.setAbstract(isAbstract);
        assertEquals(isAbstract, type.isAbstract());
    }

    @Property
    public void whenAnOntologyElementHasADirectSuper_ItIsADirectSubOfThatSuper(
            @Open GraknGraph graph, @FromGraph OntologyConcept ontologyConcept) {
        assumeFalse(Schema.MetaSchema.ROLE.getLabel().equals(ontologyConcept.getLabel()));
        OntologyConcept superType = ontologyConcept.sup();
        assertThat(directSubs(superType), hasItem(ontologyConcept));
    }

    @Property
    public void whenGettingSuperType_TheResultIsNeverItself(Type type) {
        assertNotEquals(type, type.sup());
    }

    @Property
    public void whenATypeHasAnIndirectSuperType_ItIsAnIndirectSubTypeOfThatSuperType(Type subType, long seed) {
        Type superType = choose(indirectSuperTypes(subType), seed);
        assertThat((Collection<Type>) superType.subs(), hasItem(subType));
    }

    @Property
    public void whenATypeHasAnIndirectSubType_ItIsAnIndirectSuperTypeOfThatSubType(Type superType, long seed) {
        Type subType = choose(superType.subs(), seed);
        assertThat(indirectSuperTypes(subType), hasItem(superType));
    }

    @Property
    public void whenGettingIndirectSubTypes_ReturnSelfAndIndirectSubTypesOfDirectSubTypes(
            @Open GraknGraph graph, @FromGraph Type type) {
        Collection<Type> directSubTypes = directSubs(type);
        Type[] expected = Stream.concat(
                Stream.of(type),
                directSubTypes.stream().flatMap(subType -> subType.subs().stream())
        ).toArray(Type[]::new);

        assertThat(type.subs(), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingTheIndirectSubTypes_TheyContainTheType(Type type) {
        assertThat((Collection<Type>) type.subs(), hasItem(type));
    }

    @Property
    public void whenSettingTheDirectSuperTypeOfAMetaType_Throw(
            @Meta Type subType, @FromGraph Type superType) {
        assumeTrue(sameType(subType, superType));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getLabel()));
        setDirectSuperType(subType, superType);
    }

    @Property
    public void whenSettingTheDirectSuperTypeToAnIndirectSubType_Throw(
            @Meta(false) Type type, long seed) {
        Type newSuperType = choose(type.subs(), seed);

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.loopCreated(type, newSuperType).getMessage());
        setDirectSuperType(type, newSuperType);
    }

    @Property
    public void whenSettingTheDirectSuperType_TheDirectSuperTypeIsSet(
            @Meta(false) Type subType, @FromGraph Type superType) {
        assumeTrue(sameType(subType, superType));
        assumeThat((Collection<Type>) subType.subs(), not(hasItem(superType)));

        setDirectSuperType(subType, superType);

        assertEquals(superType, subType.sup());
    }

    @Property
    public void whenAddingADirectSubTypeThatIsAMetaType_Throw(
            Type superType, @Meta @FromGraph Type subType) {
        assumeTrue(sameType(subType, superType));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getLabel()));
        addDirectSubType(superType, subType);
    }

    @Property
    public void whenAddingADirectSubTypeWhichIsAnIndirectSuperType_Throw(
            @Meta(false) Type newSubType, long seed) {
        Type type = choose(newSubType.subs(), seed);

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.loopCreated(newSubType, type).getMessage());
        addDirectSubType(type, newSubType);
    }

    @Property
    public void whenAddingADirectSubType_TheDirectSubTypeIsAdded(
            @Open GraknGraph graph, @FromGraph Type superType, @Meta(false) @FromGraph Type subType) {
        assumeTrue(sameType(subType, superType));
        assumeThat((Collection<Type>) subType.subs(), not(hasItem(superType)));

        addDirectSubType(superType, subType);

        assertThat(directSubs(superType), hasItem(subType));
    }

    @Property
    public void whenGettingIndirectInstances_ReturnDirectInstancesAndIndirectInstancesOfDirectSubTypes(
            @Open GraknGraph graph, @FromGraph Type type) {
        Collection<Type> directSubTypes = directSubs(type);
        Thing[] expected = Stream.concat(
            directInstances(type).stream(),
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
            @Meta(false) Type type, @FromGraph Role role) {
        assumeThat(type, not(is(role)));  // A role-type cannot play itself, TODO: is this sensible?

        Set<Role> previousPlays = Sets.newHashSet(type.plays());
        type.plays(role);
        Set<Role> newPlays = Sets.newHashSet(type.plays());

        assertEquals(newPlays, Sets.union(previousPlays, ImmutableSet.of(role)));
    }

    @Property
    public void whenAddingAPlaysToATypesIndirectSuperType_TheTypePlaysThatRole(
            Type type, @FromGraph Role role, long seed) {
        Type superType = choose(indirectSuperTypes(type), seed);

        assumeFalse(isMetaLabel(superType.getLabel()));
        assumeThat(superType, not(is(role)));

        Set<Role> previousPlays = Sets.newHashSet(type.plays());
        superType.plays(role);
        Set<Role> newPlays = Sets.newHashSet(type.plays());

        assertEquals(newPlays, Sets.union(previousPlays, ImmutableSet.of(role)));
    }

    @Property
    public void whenDeletingAPlaysAndTheDirectSuperTypeDoesNotPlaysThatRole_TheTypeNoLongerPlaysThatRole(
            @Meta(false) Type type, @FromGraph Role role) {
        assumeThat(type.sup().plays(), not(hasItem(role)));
        type.deletePlays(role);
        assertThat(type.plays(), not(hasItem(role)));
    }

    @Property
    public void whenDeletingAPlaysAndTheDirectSuperTypePlaysThatRole_TheTypeStillPlaysThatRole(
            @Meta(false) Type type, long seed) {
        Role role = choose(type.sup() + " plays no roles", type.sup().plays(), seed);
        type.deletePlays(role);
        assertThat(type.plays(), hasItem(role));
    }

    // TODO: Tests for `resource` and `key`
    // TODO: Tests for scope methods and inheritance

    private boolean sameType(Type type1, Type type2) {
        return type1.isEntityType() && type2.isEntityType() ||
                type1.isRelationType() && type2.isRelationType() ||
                type1.isRole() && type2.isRole() ||
                type1.isResourceType() && type2.isResourceType() ||
                type1.isRuleType() && type2.isRuleType();
    }

    private void setDirectSuperType(Type subType, Type superType) {
        if (subType.isEntityType()) {
            subType.asEntityType().sup(superType.asEntityType());
        } else if (subType.isRelationType()) {
            subType.asRelationType().sup(superType.asRelationType());
        } else if (subType.isRole()) {
            subType.asRole().sup(superType.asRole());
        } else if (subType.isResourceType()) {
            subType.asResourceType().sup(superType.asResourceType());
        } else if (subType.isRuleType()) {
            subType.asRuleType().sup(superType.asRuleType());
        } else {
            fail("unreachable");
        }
    }

    private void addDirectSubType(Type superType, Type subType) {
        if (superType.isEntityType()) {
            superType.asEntityType().sub(subType.asEntityType());
        } else if (superType.isRelationType()) {
            superType.asRelationType().sub(subType.asRelationType());
        } else if (superType.isRole()) {
            superType.asRole().sub(subType.asRole());
        } else if (superType.isResourceType()) {
            superType.asResourceType().sub(subType.asResourceType());
        } else if (superType.isRuleType()) {
            superType.asRuleType().sub(subType.asRuleType());
        } else {
            fail("unreachable");
        }
    }
}

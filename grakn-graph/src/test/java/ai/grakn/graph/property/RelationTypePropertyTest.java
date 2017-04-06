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
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.ConceptException;
import ai.grakn.generator.AbstractTypeGenerator.Meta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;

import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class RelationTypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenANonMetaRelationTypeHasNoInstancesSubTypesOrRules_ItCanBeDeleted(
            @Open GraknGraph graph, @FromGraph @Meta(false) RelationType type) {
        assumeThat(type.instances(), empty());
        assumeThat(type.subTypes(), contains(type));
        assumeThat(type.getRulesOfHypothesis(), empty());
        assumeThat(type.getRulesOfConclusion(), empty());

        type.delete();

        assertNull(graph.getType(type.getLabel()));
    }

    @Property
    public void whenAddingARelationOfAMetaType_Throw(@Meta RelationType type) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getLabel()));
        type.addRelation();
    }

    @Property
    public void whenAddingARelation_TheDirectTypeOfTheRelationIsTheTypeItWasCreatedFrom(
            @Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertEquals(type, relation.type());
    }

    @Property
    public void whenAddingARelation_TheRelationIsInNoRelations(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.relations(), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoResources(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.resources(), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoRolePlayers(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.rolePlayers(), empty());
    }

    @Property
    public void ARelationTypeRelatingARoleIsEquivalentToARoleHavingARelationType(
            RelationType relationType, @FromGraph RoleType roleType) {
        assertEquals(relationType.relates().contains(roleType), roleType.relationTypes().contains(relationType));
    }

    @Property
    public void whenMakingTheMetaRelationTypeRelateARole_Throw(
            @Meta RelationType relationType, @FromGraph RoleType roleType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(relationType.getLabel()));
        relationType.relates(roleType);
    }

    @Property
    public void whenRelatingARole_TheTypeRelatesThatRoleAndNoOtherNewRoles(
            @Meta(false) RelationType relationType, @FromGraph RoleType roleType) {
        Set<RoleType> previousHasRoles = Sets.newHashSet(relationType.relates());
        relationType.relates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(relationType.relates());

        assertEquals(Sets.union(previousHasRoles, ImmutableSet.of(roleType)), newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @Meta(false) RelationType subType, @FromGraph RoleType roleType) {
        RelationType superType = subType.superType();

        Set<RoleType> previousHasRoles = Sets.newHashSet(superType.relates());
        subType.relates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(superType.relates());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @Meta(false) RelationType subType, @FromGraph RoleType roleType) {
        RelationType superType = subType.superType();
        assumeFalse(isMetaLabel(superType.getLabel()));

        Set<RoleType> previousHasRoles = Sets.newHashSet(subType.relates());
        superType.relates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(subType.relates());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRoleFromTheMetaRelationType_Throw(
            @Meta RelationType relationType, @FromGraph RoleType roleType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(relationType.getLabel()));
        relationType.deleteRelates(roleType);
    }

    @Property
    public void whenDeletingARelatedRole_TheTypeLosesThatRoleAndNoOtherRoles(
            @Meta(false) RelationType relationType, @FromGraph RoleType roleType) {
        Set<RoleType> previousHasRoles = Sets.newHashSet(relationType.relates());
        relationType.deleteRelates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(relationType.relates());

        assertEquals(Sets.difference(previousHasRoles, ImmutableSet.of(roleType)), newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @Meta(false) RelationType subType, @FromGraph RoleType roleType) {
        RelationType superType = subType.superType();

        Set<RoleType> previousHasRoles = Sets.newHashSet(superType.relates());
        subType.deleteRelates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(superType.relates());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @Meta(false) RelationType subType, @FromGraph RoleType roleType) {
        RelationType superType = subType.superType();
        assumeFalse(isMetaLabel(superType.getLabel()));

        Set<RoleType> previousHasRoles = Sets.newHashSet(subType.relates());
        superType.deleteRelates(roleType);
        Set<RoleType> newHasRoles = Sets.newHashSet(subType.relates());

        assertEquals(previousHasRoles, newHasRoles);
    }
}

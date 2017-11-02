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

package ai.grakn.test.property.kb;

import ai.grakn.GraknTx;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.generator.AbstractSchemaConceptGenerator.Meta;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.AbstractTypeGenerator.NonAbstract;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs.Open;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;

import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static java.util.stream.Collectors.toSet;
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
public class RelationshipTypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenANonMetaRelationTypeHasNoInstancesSubTypesOrRules_ItCanBeDeleted(
            @Open GraknTx graph, @FromTx @NonMeta RelationshipType type) {
        assumeThat(type.instances().collect(toSet()), empty());
        assumeThat(type.subs().collect(toSet()), contains(type));
        assumeThat(type.getRulesOfHypothesis().collect(toSet()), empty());
        assumeThat(type.getRulesOfConclusion().collect(toSet()), empty());

        type.delete();

        assertNull(graph.getSchemaConcept(type.getLabel()));
    }

    @Property
    public void whenAddingARelationOfAMetaType_Throw(@Meta RelationshipType type) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.addRelationship();
    }

    @Property
    public void whenAddingARelation_TheDirectTypeOfTheRelationIsTheTypeItWasCreatedFrom(
            @NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.addRelationship();

        assertEquals(type, relationship.type());
    }

    @Property
    public void whenAddingARelation_TheRelationIsInNoRelations(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.addRelationship();

        assertThat(relationship.relationships().collect(toSet()), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoResources(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.addRelationship();

        assertThat(relationship.attributes().collect(toSet()), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoRolePlayers(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.addRelationship();

        assertThat(relationship.rolePlayers().collect(toSet()), empty());
    }

    @Property
    public void relationTypeRelatingARoleIsEquivalentToARoleHavingARelationType(
            RelationshipType relationshipType, @FromTx Role role) {
        assertEquals(relationshipType.relates().collect(toSet()).contains(role), role.relationshipTypes().collect(toSet()).contains(relationshipType));
    }

    @Property
    public void whenMakingTheMetaRelationTypeRelateARole_Throw(@Meta RelationshipType relationshipType, @FromTx Role role) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(relationshipType.getLabel()).getMessage());
        relationshipType.relates(role);
    }

    @Property
    public void whenRelatingARole_TheTypeRelatesThatRoleAndNoOtherNewRoles(
            @NonMeta RelationshipType relationshipType, @FromTx Role role) {
        Set<Role> previousHasRoles = relationshipType.relates().collect(toSet());
        relationshipType.relates(role);
        Set<Role> newHasRoles = relationshipType.relates().collect(toSet());

        assertEquals(Sets.union(previousHasRoles, ImmutableSet.of(role)), newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();

        Set<Role> previousHasRoles = superType.relates().collect(toSet());
        subType.relates(role);
        Set<Role> newHasRoles = superType.relates().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();
        assumeFalse(isMetaLabel(superType.getLabel()));

        Set<Role> previousHasRoles = subType.relates().collect(toSet());
        superType.relates(role);
        Set<Role> newHasRoles = subType.relates().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRoleFromTheMetaRelationType_Throw(
            @Meta RelationshipType relationshipType, @FromTx Role role) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(relationshipType.getLabel()).getMessage());
        relationshipType.deleteRelates(role);
    }

    @Property
    public void whenDeletingARelatedRole_TheTypeLosesThatRoleAndNoOtherRoles(
            @NonMeta RelationshipType relationshipType, @FromTx Role role) {
        Set<Role> previousHasRoles = relationshipType.relates().collect(toSet());
        relationshipType.deleteRelates(role);
        Set<Role> newHasRoles = relationshipType.relates().collect(toSet());

        assertEquals(Sets.difference(previousHasRoles, ImmutableSet.of(role)), newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();

        Set<Role> previousHasRoles = superType.relates().collect(toSet());
        subType.deleteRelates(role);
        Set<Role> newHasRoles = superType.relates().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();
        assumeFalse(isMetaLabel(superType.getLabel()));

        Set<Role> previousHasRoles = subType.relates().collect(toSet());
        superType.deleteRelates(role);
        Set<Role> newHasRoles = subType.relates().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }
}
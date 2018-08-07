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
        assumeThat(type.whenRules().collect(toSet()), empty());
        assumeThat(type.thenRules().collect(toSet()), empty());

        type.delete();

        assertNull(graph.getSchemaConcept(type.label()));
    }

    @Property
    public void whenAddingARelationOfAMetaType_Throw(@Meta RelationshipType type) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(type.label()).getMessage());
        type.create();
    }

    @Property
    public void whenAddingARelation_TheDirectTypeOfTheRelationIsTheTypeItWasCreatedFrom(
            @NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.create();

        assertEquals(type, relationship.type());
    }

    @Property
    public void whenAddingARelation_TheRelationIsInNoRelations(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.create();

        assertThat(relationship.relationships().collect(toSet()), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoResources(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.create();

        assertThat(relationship.attributes().collect(toSet()), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoRolePlayers(@NonMeta @NonAbstract RelationshipType type) {
        Relationship relationship = type.create();

        assertThat(relationship.rolePlayers().collect(toSet()), empty());
    }

    @Property
    public void relationTypeRelatingARoleIsEquivalentToARoleHavingARelationType(
            RelationshipType relationshipType, @FromTx Role role) {
        assertEquals(relationshipType.roles().collect(toSet()).contains(role), role.relationships().collect(toSet()).contains(relationshipType));
    }

    @Property
    public void whenMakingTheMetaRelationTypeRelateARole_Throw(@Meta RelationshipType relationshipType, @FromTx Role role) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(relationshipType.label()).getMessage());
        relationshipType.relates(role);
    }

    @Property
    public void whenRelatingARole_TheTypeRelatesThatRoleAndNoOtherNewRoles(
            @NonMeta RelationshipType relationshipType, @FromTx Role role) {
        Set<Role> previousHasRoles = relationshipType.roles().collect(toSet());
        relationshipType.relates(role);
        Set<Role> newHasRoles = relationshipType.roles().collect(toSet());

        assertEquals(Sets.union(previousHasRoles, ImmutableSet.of(role)), newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();

        Set<Role> previousHasRoles = superType.roles().collect(toSet());
        subType.relates(role);
        Set<Role> newHasRoles = superType.roles().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenRelatingARole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();
        assumeFalse(isMetaLabel(superType.label()));

        Set<Role> previousHasRoles = subType.roles().collect(toSet());
        superType.relates(role);
        Set<Role> newHasRoles = subType.roles().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRoleFromTheMetaRelationType_Throw(
            @Meta RelationshipType relationshipType, @FromTx Role role) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(relationshipType.label()).getMessage());
        relationshipType.unrelate(role);
    }

    @Property
    public void whenDeletingARelatedRole_TheTypeLosesThatRoleAndNoOtherRoles(
            @NonMeta RelationshipType relationshipType, @FromTx Role role) {
        Set<Role> previousHasRoles = relationshipType.roles().collect(toSet());
        relationshipType.unrelate(role);
        Set<Role> newHasRoles = relationshipType.roles().collect(toSet());

        assertEquals(Sets.difference(previousHasRoles, ImmutableSet.of(role)), newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSuperTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();

        Set<Role> previousHasRoles = superType.roles().collect(toSet());
        subType.unrelate(role);
        Set<Role> newHasRoles = superType.roles().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }

    @Property
    public void whenDeletingARelatedRole_TheDirectSubTypeRelatedRolesAreUnchanged(
            @NonMeta RelationshipType subType, @FromTx Role role) {
        RelationshipType superType = subType.sup();
        assumeFalse(isMetaLabel(superType.label()));

        Set<Role> previousHasRoles = subType.roles().collect(toSet());
        superType.unrelate(role);
        Set<Role> newHasRoles = subType.roles().collect(toSet());

        assertEquals(previousHasRoles, newHasRoles);
    }
}
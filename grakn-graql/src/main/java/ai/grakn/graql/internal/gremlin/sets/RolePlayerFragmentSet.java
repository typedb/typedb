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

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.util.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Describes the edge connecting a {@link Relationship} to a role-player.
 * <p>
 * Can be constrained with information about the possible {@link Role}s or {@link RelationshipType}s.
 *
 * @author Felix Chapman
 */
class RolePlayerFragmentSet extends EquivalentFragmentSet {

    private final Var relation;
    private final Var edge;
    private final Var rolePlayer;
    private final @Nullable Var role;
    private final @Nullable ImmutableSet<Label> roleTypeLabels;
    private final @Nullable ImmutableSet<Label> relationTypeLabels;
    private final VarProperty varProperty;

    RolePlayerFragmentSet(VarProperty varProperty,
                          Var relation, Var edge, Var rolePlayer, @Nullable Var role,
                          @Nullable ImmutableSet<Label> roleLabels, @Nullable ImmutableSet<Label> relationTypeLabels) {
        super(
                Fragments.inRolePlayer(varProperty, rolePlayer, edge, relation, role, roleLabels, relationTypeLabels),
                Fragments.outRolePlayer(varProperty, relation, edge, rolePlayer, role, roleLabels, relationTypeLabels)
        );
        this.relation = relation;
        this.edge = edge;
        this.rolePlayer = rolePlayer;
        this.role = role;
        this.roleTypeLabels = roleLabels;
        this.relationTypeLabels = relationTypeLabels;
        this.varProperty = varProperty;
    }

    /**
     * A query can use the role-type labels on a {@link Schema.EdgeLabel#ROLE_PLAYER} edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link RolePlayerFragmentSet} {@code $r-[role-player:$e role:$R ...]->$p}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo]}
     * </ol>
     *
     * When these criteria are met, the {@link RolePlayerFragmentSet} can be filtered to the indirect sub-types of
     * {@code foo} and will no longer need to navigate to the {@link Role} directly:
     * <p>
     * {@code $r-[role-player:$e roles:foo ...]->$p}
     * <p>
     * In the special case where the role is specified as the meta {@code role}, no labels are added and the {@link Role}
     * variable is detached from the {@link Schema.EdgeLabel#ROLE_PLAYER} edge.
     * <p>
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     */
    static boolean applyRolePlayerRoleOptimisation(Collection<EquivalentFragmentSet> fragmentSets, GraknTx graph) {
        Iterable<RolePlayerFragmentSet> rolePlayers = EquivalentFragmentSets.fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)::iterator;

        for (RolePlayerFragmentSet rolePlayer : rolePlayers) {
            Var roleVar = rolePlayer.role;

            if (roleVar == null) continue;

            @Nullable LabelFragmentSet roleLabel = EquivalentFragmentSets.typeLabelOf(roleVar, fragmentSets);

            if (roleLabel == null) continue;

            @Nullable RolePlayerFragmentSet newRolePlayer = null;

            if (roleLabel.label().equals(Schema.MetaSchema.ROLE.getLabel())) {
                newRolePlayer = rolePlayer.removeRoleVar();
            } else {
                SchemaConcept schemaConcept = graph.getSchemaConcept(roleLabel.label());

                if (schemaConcept != null && schemaConcept.isRole()) {
                    Role role = schemaConcept.asRole();
                    newRolePlayer = rolePlayer.substituteRoleTypeLabel(role);
                }
            }

            if (newRolePlayer != null) {
                fragmentSets.remove(rolePlayer);
                fragmentSets.add(newRolePlayer);
                return true;
            }
        }

        return false;
    }

    /**
     * A query can use the {@link RelationshipType} {@link Label}s on a {@link Schema.EdgeLabel#ROLE_PLAYER} edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link RolePlayerFragmentSet} {@code $r-[role-player:$e ...]->$p}
     *         without any {@link RelationshipType} {@link Label}s specified
     *     <li>There is a {@link IsaFragmentSet} {@code $r-[isa]->$R}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo]}
     * </ol>
     *
     * When these criteria are met, the {@link RolePlayerFragmentSet} can be filtered to the indirect sub-types of
     * {@code foo}.
     * <p>
     * {@code $r-[role-player:$e rels:foo]->$p}
     * <p>
     *
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     * <p>
     * We also keep the {@link IsaFragmentSet}, although the results will still be correct without it. This is because
     * it can help with performance: there are some instances where it makes sense to navigate from the {@link RelationshipType}
     * {@code foo} to all instances. In order to do that, the {@link IsaFragmentSet} must be present.
     */
    static boolean applyRolePlayerRelationTypeOptimisation(Collection<EquivalentFragmentSet> fragmentSets, GraknTx graph) {
        Iterable<RolePlayerFragmentSet> rolePlayers = EquivalentFragmentSets.fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)::iterator;

        for (RolePlayerFragmentSet rolePlayer : rolePlayers) {

            if (rolePlayer.relationTypeLabels != null) continue;

            @Nullable IsaFragmentSet isa = EquivalentFragmentSets.typeInformationOf(rolePlayer.relation, fragmentSets);

            if (isa == null) continue;

            @Nullable LabelFragmentSet relationLabel = EquivalentFragmentSets.typeLabelOf(isa.type(), fragmentSets);

            if (relationLabel == null) continue;

            SchemaConcept schemaConcept = graph.getSchemaConcept(relationLabel.label());

            if (schemaConcept != null && schemaConcept.isRelationshipType()) {
                RelationshipType relationshipType = schemaConcept.asRelationshipType();

                fragmentSets.remove(rolePlayer);
                fragmentSets.add(rolePlayer.addRelationTypeLabel(relationshipType));

                return true;
            }
        }

        return false;
    }

    /**
     * Apply an optimisation where we check the {@link Role} property instead of navigating to the {@link Role} directly.
     * @param role the {@link Role} that this role-player fragment must link to
     * @return a new {@link RolePlayerFragmentSet} with the same properties excepting role-types
     */
    private RolePlayerFragmentSet substituteRoleTypeLabel(Role role) {
        Preconditions.checkNotNull(this.role);
        Preconditions.checkState(roleTypeLabels == null);

        ImmutableSet<Label> newRoleLabels = role.subs().map(SchemaConcept::getLabel).collect(toImmutableSet());

        return new RolePlayerFragmentSet(varProperty,
                relation, edge, rolePlayer, null, newRoleLabels, relationTypeLabels
        );
    }

    /**
     * Apply an optimisation where we check the {@link RelationshipType} property.
     * @param relationshipType the {@link RelationshipType} that this role-player fragment must link to
     * @return a new {@link RolePlayerFragmentSet} with the same properties excepting relation-type labels
     */
    private RolePlayerFragmentSet addRelationTypeLabel(RelationshipType relationshipType) {
        Preconditions.checkState(relationTypeLabels == null);

        ImmutableSet<Label> newRelationLabels = relationshipType.subs().map(Type::getLabel).collect(toImmutableSet());

        return new RolePlayerFragmentSet(varProperty,
                relation, edge, rolePlayer, role, roleTypeLabels, newRelationLabels
        );
    }

    /**
     * Remove any specified role variable
     */
    private RolePlayerFragmentSet removeRoleVar() {
        Preconditions.checkNotNull(role);
        return new RolePlayerFragmentSet(varProperty, relation, edge, rolePlayer, null, roleTypeLabels, relationTypeLabels);
    }
}

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

package grakn.core.graql.internal.gremlin.sets;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.server.kb.Schema;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.Fragments;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toSet;

/**
 * @see EquivalentFragmentSets#rolePlayer(VarProperty, Variable, Variable, Variable, Variable)
 *
 */
@AutoValue
abstract class RolePlayerFragmentSet extends EquivalentFragmentSet {

    public static RolePlayerFragmentSet of(
            VarProperty varProperty, Variable relation, Variable edge, Variable rolePlayer, @Nullable Variable role,
            @Nullable ImmutableSet<Label> roleLabels, @Nullable ImmutableSet<Label> relationTypeLabels
    ) {
        return new AutoValue_RolePlayerFragmentSet(
                varProperty, relation, edge, rolePlayer, role, roleLabels, relationTypeLabels
        );
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.inRolePlayer(varProperty(), rolePlayer(), edge(), relation(), role(), roleLabels(), relationshipTypeLabels()),
                Fragments.outRolePlayer(varProperty(), relation(), edge(), rolePlayer(), role(), roleLabels(), relationshipTypeLabels())
        );
    }

    abstract Variable relation();
    abstract Variable edge();
    abstract Variable rolePlayer();
    abstract @Nullable
    Variable role();
    abstract @Nullable ImmutableSet<Label> roleLabels();
    abstract @Nullable ImmutableSet<Label> relationshipTypeLabels();

    /**
     * A query can use the role-type labels on a {@link Schema.EdgeLabel#ROLE_PLAYER} edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link RolePlayerFragmentSet} {@code $r-[role-player:$e role:$R ...]->$p}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo,bar]}
     * </ol>
     *
     * When these criteria are met, the {@link RolePlayerFragmentSet} can be filtered to the indirect sub-types of
     * {@code foo} and {@code bar} and will no longer need to navigate to the {@link Role} directly:
     * <p>
     * {@code $r-[role-player:$e roles:foo,bar ...]->$p}
     * <p>
     * In the special case where the role is specified as the meta {@code role}, no labels are added and the {@link Role}
     * variable is detached from the {@link Schema.EdgeLabel#ROLE_PLAYER} edge.
     * <p>
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     */
    static final FragmentSetOptimisation ROLE_OPTIMISATION = (fragmentSets, tx) -> {
        Iterable<RolePlayerFragmentSet> rolePlayers =
                EquivalentFragmentSets.fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)::iterator;

        for (RolePlayerFragmentSet rolePlayer : rolePlayers) {
            Variable roleVar = rolePlayer.role();

            if (roleVar == null) continue;

            @Nullable LabelFragmentSet roleLabel = EquivalentFragmentSets.labelOf(roleVar, fragmentSets);

            if (roleLabel == null) continue;

            @Nullable RolePlayerFragmentSet newRolePlayer = null;

            if (roleLabel.labels().equals(ImmutableSet.of(Schema.MetaSchema.ROLE.getLabel()))) {
                newRolePlayer = rolePlayer.removeRoleVar();
            } else {
                Set<SchemaConcept> concepts = roleLabel.labels().stream()
                        .map(tx::<SchemaConcept>getSchemaConcept)
                        .collect(toSet());

                if (concepts.stream().allMatch(schemaConcept -> schemaConcept != null && schemaConcept.isRole())) {
                    Stream<Role> roles = concepts.stream().map(Concept::asRole);
                    newRolePlayer = rolePlayer.substituteRoleLabel(roles);
                }
            }

            if (newRolePlayer != null) {
                fragmentSets.remove(rolePlayer);
                fragmentSets.add(newRolePlayer);
                return true;
            }
        }

        return false;
    };

    /**
     * For traversals associated with implicit relations (either via the has keyword or directly using the implicit relation):
     *
     * - expands the roles and relation types to include relevant hierarchies
     * - as we have transaction information here, we additionally filter the non-relevant variant between key- and has- attribute options.
     *
     */
    static final FragmentSetOptimisation IMPLICIT_RELATION_OPTIMISATION = (fragmentSets, tx) -> {
        Iterable<RolePlayerFragmentSet> rolePlayers =
                EquivalentFragmentSets.fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)::iterator;

        for (RolePlayerFragmentSet rolePlayer : rolePlayers) {
            @Nullable RolePlayerFragmentSet newRolePlayer = null;

            @Nullable ImmutableSet<Label> relLabels = rolePlayer.relationshipTypeLabels();
            @Nullable ImmutableSet<Label> roleLabels = rolePlayer.roleLabels();
            if(relLabels == null || roleLabels == null) continue;

            Set<RelationType> relTypes = relLabels.stream()
                    .map(tx::<SchemaConcept>getSchemaConcept)
                    .filter(Objects::nonNull)
                    .filter(Concept::isRelationType)
                    .map(Concept::asRelationType)
                    .collect(toSet());

            Set<Role> roles = roleLabels.stream()
                    .map(tx::<SchemaConcept>getSchemaConcept)
                    .filter(Objects::nonNull)
                    .filter(Concept::isRole)
                    .map(Concept::asRole)
                    .collect(toSet());
            if (Stream.concat(relTypes.stream(), roles.stream()).allMatch(SchemaConcept::isImplicit)) {
                newRolePlayer = rolePlayer.substituteLabels(roles, relTypes);
            }

            if (newRolePlayer != null && !newRolePlayer.equals(rolePlayer)) {
                fragmentSets.remove(rolePlayer);
                fragmentSets.add(newRolePlayer);
                return true;
            }
        }

        return false;
    };

    /**
     * A query can use the {@link RelationType} {@link Label}s on a {@link Schema.EdgeLabel#ROLE_PLAYER} edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link RolePlayerFragmentSet} {@code $r-[role-player:$e ...]->$p}
     *         without any {@link RelationType} {@link Label}s specified
     *     <li>There is a {@link IsaFragmentSet} {@code $r-[isa]->$R}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo,bar]}
     * </ol>
     *
     * When these criteria are met, the {@link RolePlayerFragmentSet} can be filtered to the types
     * {@code foo} and {@code bar}.
     * <p>
     * {@code $r-[role-player:$e rels:foo]->$p}
     * <p>
     *
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     * <p>
     * We also keep the {@link IsaFragmentSet}, although the results will still be correct without it. This is because
     * it can help with performance: there are some instances where it makes sense to navigate from the {@link RelationType}
     * {@code foo} to all instances. In order to do that, the {@link IsaFragmentSet} must be present.
     */
    static final FragmentSetOptimisation RELATION_TYPE_OPTIMISATION = (fragmentSets, graph) -> {
        Iterable<RolePlayerFragmentSet> rolePlayers =
                EquivalentFragmentSets.fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)::iterator;

        for (RolePlayerFragmentSet rolePlayer : rolePlayers) {

            if (rolePlayer.relationshipTypeLabels() != null) continue;

            @Nullable IsaFragmentSet isa = EquivalentFragmentSets.typeInformationOf(rolePlayer.relation(), fragmentSets);

            if (isa == null) continue;

            @Nullable LabelFragmentSet relationLabel = EquivalentFragmentSets.labelOf(isa.type(), fragmentSets);

            if (relationLabel == null) continue;

            Stream<SchemaConcept> concepts =
                    relationLabel.labels().stream().map(graph::<SchemaConcept>getSchemaConcept);

            if (concepts.allMatch(schemaConcept -> schemaConcept != null && schemaConcept.isRelationType())) {
                fragmentSets.remove(rolePlayer);
                fragmentSets.add(rolePlayer.addRelationshipTypeLabels(relationLabel.labels()));

                return true;
            }
        }

        return false;
    };

    private RolePlayerFragmentSet substituteLabels(Set<Role> roles, Set<RelationType> relTypes){
        ImmutableSet<Label> newRoleTypeLabels = relTypes != null?
                relTypes.stream().flatMap(RelationType::subs).map(SchemaConcept::label).collect(toImmutableSet()) :
                null;
        ImmutableSet<Label> newRoleLabels = roles != null?
                roles.stream().flatMap(Role::subs).map(SchemaConcept::label).collect(toImmutableSet()) :
                null;

        return new AutoValue_RolePlayerFragmentSet(
                varProperty(), relation(), edge(), rolePlayer(), null,
                newRoleLabels!= null? newRoleLabels : roleLabels(),
                newRoleTypeLabels != null? newRoleTypeLabels : relationshipTypeLabels()
        );
    }

    /**
     * NB: doesn't allow overwrites
     * Apply an optimisation where we check the {@link Role} property instead of navigating to the {@link Role} directly.
     * @param roles the role-player must link to any of these (or their sub-types)
     * @return a new {@link RolePlayerFragmentSet} with the same properties excepting role-types
     */
    private RolePlayerFragmentSet substituteRoleLabel(Stream<Role> roles) {
        Preconditions.checkNotNull(this.role());
        Preconditions.checkState(roleLabels() == null);

        ImmutableSet<Label> newRoleLabels =
                roles.flatMap(Role::subs).map(SchemaConcept::label).collect(toImmutableSet());

        return new AutoValue_RolePlayerFragmentSet(
                varProperty(), relation(), edge(), rolePlayer(), null, newRoleLabels, relationshipTypeLabels()
        );
    }

    /**
     * NB: doesn't allow overwrites
     * Apply an optimisation where we check the {@link RelationType} property.
     * @param relTypeLabels the role-player fragment must link to any of these (not including sub-types)
     * @return a new {@link RolePlayerFragmentSet} with the same properties excepting relation-type labels
     */
    private RolePlayerFragmentSet addRelationshipTypeLabels(ImmutableSet<Label> relTypeLabels) {
        Preconditions.checkState(relationshipTypeLabels() == null);

        return new AutoValue_RolePlayerFragmentSet(
                varProperty(),
                relation(), edge(), rolePlayer(), role(), roleLabels(), relTypeLabels
        );
    }

    /**
     * Remove any specified role variable
     */
    private RolePlayerFragmentSet removeRoleVar() {
        Preconditions.checkNotNull(role());
        return new AutoValue_RolePlayerFragmentSet(
                varProperty(), relation(), edge(), rolePlayer(), null, roleLabels(), relationshipTypeLabels()
        );
    }
}

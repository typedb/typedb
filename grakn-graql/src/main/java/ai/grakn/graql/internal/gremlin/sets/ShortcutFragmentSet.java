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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.util.Schema;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Describes the edge connecting a relation to a role-player.
 * <p>
 * Can be constrained with information about the possible role types or relation types.
 *
 * @author Felix Chapman
 */
class ShortcutFragmentSet extends EquivalentFragmentSet {

    private final Var relation;
    private final Var edge;
    private final Var rolePlayer;
    private final Optional<Var> role;
    private final Optional<Set<Label>> roleTypeLabels;
    private final Optional<Set<Label>> relationTypeLabels;
    private final VarProperty varProperty;

    ShortcutFragmentSet(VarProperty varProperty,
            Var relation, Var edge, Var rolePlayer, Optional<Var> role,
            Optional<Set<Label>> roleLabels, Optional<Set<Label>> relationTypeLabels) {
        super(
                Fragments.inShortcut(varProperty, rolePlayer, edge, relation, role, roleLabels, relationTypeLabels),
                Fragments.outShortcut(varProperty, relation, edge, rolePlayer, role, roleLabels, relationTypeLabels)
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
     * A query can use the role-type labels on a shortcut edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link ShortcutFragmentSet} {@code $r-[shortcut:$e role:$R ...]->$p}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo]}
     * </ol>
     *
     * When these criteria are met, the {@link ShortcutFragmentSet} can be filtered to the indirect sub-types of
     * {@code foo} and will no longer need to navigate to the role-type directly:
     * <p>
     * {@code $r-[shortcut:$e roles:foo ...]->$p}
     * <p>
     * In the special case where the role is specified as the meta {@code role}, no labels are added and the role
     * variable is detached from the shortcut edge.
     * <p>
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     */
    static boolean applyShortcutRoleOptimisation(Collection<EquivalentFragmentSet> fragmentSets, GraknGraph graph) {
        Iterable<ShortcutFragmentSet> shortcuts = EquivalentFragmentSets.fragmentSetOfType(ShortcutFragmentSet.class, fragmentSets)::iterator;

        for (ShortcutFragmentSet shortcut : shortcuts) {
            Optional<Var> roleVar = shortcut.role;

            if (!roleVar.isPresent()) continue;

            @Nullable LabelFragmentSet roleLabel = EquivalentFragmentSets.typeLabelOf(roleVar.get(), fragmentSets);

            if (roleLabel == null) continue;

            @Nullable ShortcutFragmentSet newShortcut = null;

            if (roleLabel.label().equals(Schema.MetaSchema.ROLE.getLabel())) {
                newShortcut = shortcut.removeRoleVar();
            } else {
                OntologyConcept ontologyConcept = graph.getOntologyConcept(roleLabel.label());

                if (ontologyConcept != null && ontologyConcept.isRole()) {
                    Role role = ontologyConcept.asRole();
                    newShortcut = shortcut.substituteRoleTypeLabel(role);
                }
            }

            if (newShortcut != null) {
                fragmentSets.remove(shortcut);
                fragmentSets.add(newShortcut);
                return true;
            }
        }

        return false;
    }

    /**
     * A query can use the relation-type labels on a shortcut edge when the following criteria are met:
     * <ol>
     *     <li>There is a {@link ShortcutFragmentSet} {@code $r-[shortcut:$e ...]->$p}
     *         without any relation type labels specified
     *     <li>There is a {@link IsaFragmentSet} {@code $r-[isa]->$R}
     *     <li>There is a {@link LabelFragmentSet} {@code $R[label:foo]}
     * </ol>
     *
     * When these criteria are met, the {@link ShortcutFragmentSet} can be filtered to the indirect sub-types of
     * {@code foo}.
     * <p>
     * {@code $r-[shortcut:$e rels:foo]->$p}
     * <p>
     *
     * However, we must still retain the {@link LabelFragmentSet} because it is possible it is selected as a result or
     * referred to elsewhere in the query.
     * <p>
     * We also keep the {@link IsaFragmentSet}, although the results will still be correct without it. This is because
     * it can help with performance: there are some instances where it makes sense to navigate from the relation-type
     * {@code foo} to all instances. In order to do that, the {@link IsaFragmentSet} must be present.
     */
    static boolean applyShortcutRelationTypeOptimisation(Collection<EquivalentFragmentSet> fragmentSets, GraknGraph graph) {
        Iterable<ShortcutFragmentSet> shortcuts = EquivalentFragmentSets.fragmentSetOfType(ShortcutFragmentSet.class, fragmentSets)::iterator;

        for (ShortcutFragmentSet shortcut : shortcuts) {

            if (shortcut.relationTypeLabels.isPresent()) continue;

            @Nullable IsaFragmentSet isa = EquivalentFragmentSets.typeInformationOf(shortcut.relation, fragmentSets);

            if (isa == null) continue;

            @Nullable LabelFragmentSet relationLabel = EquivalentFragmentSets.typeLabelOf(isa.type(), fragmentSets);

            if (relationLabel == null) continue;

            OntologyConcept ontologyConcept = graph.getOntologyConcept(relationLabel.label());

            if (ontologyConcept != null && ontologyConcept.isRelationType()) {
                RelationType relationType = ontologyConcept.asRelationType();

                fragmentSets.remove(shortcut);
                fragmentSets.add(shortcut.addRelationTypeLabel(relationType));

                return true;
            }
        }

        return false;
    }

    /**
     * Apply an optimisation where we check the role-type property instead of navigating to the role-type directly.
     * @param role the role-type that this shortcut fragment must link to
     * @return a new {@link ShortcutFragmentSet} with the same properties excepting role-types
     */
    private ShortcutFragmentSet substituteRoleTypeLabel(Role role) {
        Preconditions.checkState(this.role.isPresent());
        Preconditions.checkState(!roleTypeLabels.isPresent());

        Set<Label> newRoleLabels = role.subs().stream().map(OntologyConcept::getLabel).collect(toSet());

        return new ShortcutFragmentSet(varProperty,
                relation, edge, rolePlayer, Optional.empty(), Optional.of(newRoleLabels), relationTypeLabels
        );
    }

    /**
     * Apply an optimisation where we check the relation-type property.
     * @param relationType the relation-type that this shortcut fragment must link to
     * @return a new {@link ShortcutFragmentSet} with the same properties excepting relation-type labels
     */
    private ShortcutFragmentSet addRelationTypeLabel(RelationType relationType) {
        Preconditions.checkState(!relationTypeLabels.isPresent());

        Set<Label> newRelationLabels = relationType.subs().stream().map(Type::getLabel).collect(toSet());

        return new ShortcutFragmentSet(varProperty,
                relation, edge, rolePlayer, role, roleTypeLabels, Optional.of(newRelationLabels)
        );
    }

    /**
     * Remove any specified role variable
     */
    private ShortcutFragmentSet removeRoleVar() {
        Preconditions.checkState(role.isPresent());
        return new ShortcutFragmentSet(varProperty, relation, edge, rolePlayer, Optional.empty(), roleTypeLabels, relationTypeLabels);
    }
}

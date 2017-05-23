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

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.base.Preconditions;

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
    private final Optional<Var> roleType;
    private final Optional<Set<TypeLabel>> roleTypeLabels;
    private final Optional<Set<TypeLabel>> relationTypeLabels;

    ShortcutFragmentSet(
            Var relation, Var edge, Var rolePlayer, Optional<Var> roleType,
            Optional<Set<TypeLabel>> roleTypeLabels, Optional<Set<TypeLabel>> relationTypeLabels) {
        super(
                Fragments.inShortcut(rolePlayer, edge, relation, roleType, roleTypeLabels, relationTypeLabels),
                Fragments.outShortcut(relation, edge, rolePlayer, roleType, roleTypeLabels, relationTypeLabels)
        );
        this.relation = relation;
        this.edge = edge;
        this.rolePlayer = rolePlayer;
        this.roleType = roleType;
        this.roleTypeLabels = roleTypeLabels;
        this.relationTypeLabels = relationTypeLabels;
    }

    Var relation() {
        return relation;
    }

    Optional<Var> roleType() {
        return roleType;
    }

    Optional<Set<TypeLabel>> relationTypeLabels() {
        return relationTypeLabels;
    }

    /**
     * Apply an optimisation where we check the role-type property instead of navigating to the role-type directly.
     * @param roleType the role-type that this shortcut fragment must link to
     * @return a new {@link ShortcutFragmentSet} with the same properties excepting role-types
     */
    ShortcutFragmentSet substituteRoleTypeLabel(RoleType roleType) {
        Preconditions.checkState(this.roleType.isPresent());
        Preconditions.checkState(!roleTypeLabels.isPresent());

        Set<TypeLabel> newRoleTypeLabels = roleType.subTypes().stream().map(Type::getLabel).collect(toSet());

        return new ShortcutFragmentSet(
                relation, edge, rolePlayer, Optional.empty(), Optional.of(newRoleTypeLabels), relationTypeLabels
        );
    }

    /**
     * Apply an optimisation where we check the relation-type property.
     * @param relationType the relation-type that this shortcut fragment must link to
     * @return a new {@link ShortcutFragmentSet} with the same properties excepting relation-type labels
     */
    ShortcutFragmentSet addRelationTypeLabel(RelationType relationType) {
        Preconditions.checkState(!relationTypeLabels.isPresent());

        Set<TypeLabel> newRelationTypeLabels = relationType.subTypes().stream().map(Type::getLabel).collect(toSet());

        return new ShortcutFragmentSet(
                relation, edge, rolePlayer, roleType, roleTypeLabels, Optional.of(newRelationTypeLabels)
        );
    }
}

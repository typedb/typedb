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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.hasDirectSubTypes;

/**
 * A query can use a shortcut edge traversal when the following criteria are met:
 *
 * <ol>
 *  <li>There is a {@link CastingFragmentSet} from {@code r} to {@code c}</li>
 *  <li>There is a {@link RolePlayerFragmentSet} from {@code c} to {@code x}</li>
 *  <li>If there is a {@link IsaFragmentSet} from {@code c} to {@code C}, then {@code C} must have a
 *  {@link LabelFragmentSet}</li>
 *  <li>If there is a {@link IsaFragmentSet} from {@code r} to {@code R} then {@code R} must have no direct
 *  sub-types</li>
 * </ol>
 *
 * And optionally:
 *
 * <ol>
 *  <li>There is a {@link IsaFragmentSet} from {@code c} to a type with a {@link LabelFragmentSet} and no direct
 *  sub-types</li>
 *  <li>There is a {@link IsaFragmentSet} from {@code r} to a type with a {@link LabelFragmentSet}</li>
 * </ol>
 *
 * We assume that {@code c} is otherwise never referred to in the query, since it's a casting.
 *
 * When these criteria are met, all the fragments can be replaced with a {@link ShortcutFragmentSet} from {@code r}
 * to {@code x}, with optionally specified role- and relation-types.
 *
 * @author Felix Chapman
 */
class ShortcutFragmentSet extends EquivalentFragmentSet {

    ShortcutFragmentSet(
            VarName relation, VarName edge, VarName rolePlayer, Optional<TypeLabel> roleType,
            Optional<TypeLabel> relationType) {
        super(
                Fragments.inShortcut(rolePlayer, edge, relation, roleType, relationType),
                Fragments.outShortcut(relation, edge, rolePlayer, roleType, relationType)
        );
    }

    static boolean applyShortcutOptimisation(Collection<EquivalentFragmentSet> fragmentSets, GraknGraph graph) {
        Iterable<CastingFragmentSet> castingFragmentSets =
                fragmentSetOfType(CastingFragmentSet.class, fragmentSets)::iterator;

        for (CastingFragmentSet castingFragmentSet : castingFragmentSets) {
            if (attemptOptimiseCasting(fragmentSets, graph, castingFragmentSet)) {
                return true;
            }
        }

        return false;
    }

    private static boolean attemptOptimiseCasting(Collection<EquivalentFragmentSet> fragmentSets, GraknGraph graph, CastingFragmentSet castingFragmentSet) {
        VarName relation = castingFragmentSet.relation();
        VarName casting = castingFragmentSet.casting();

        RolePlayerFragmentSet rolePlayerFragmentSet = findRolePlayerFragmentSet(fragmentSets, casting);

        // Try and get type of relation
        Optional<IsaFragmentSet> relIsaFragment = fragmentSetOfType(IsaFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.instance().equals(relation))
                .findAny();

        Optional<TypeLabel> relType = relIsaFragment
                .map(IsaFragmentSet::type)
                .flatMap(type -> findTypeLabel(fragmentSets, type));

        // We can't use the shortcut's relation type label if the relation type has sub-types, because we don't know
        // precisely which type the shortcut edge label will have. However, we can still use the shortcut edge and
        // check the type of the relation the old fashioned way.
        relType = relType.filter(type -> !hasDirectSubTypes(graph, type));

        // Try and get role type
        Optional<IsaCastingsFragmentSet> castingIsaFragment = fragmentSetOfType(IsaCastingsFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.casting().equals(casting))
                .findAny();

        Optional<TypeLabel> roleType = castingIsaFragment
                .map(IsaCastingsFragmentSet::roleType)
                .flatMap(type -> findTypeLabel(fragmentSets, type));

        if (castingIsaFragment.isPresent() && !roleType.isPresent()) {
            return false;
        }

        // When the meta role-type is specified, it's the same as not specifying the role at all
        if (roleType.isPresent() && roleType.get().equals(Schema.MetaSchema.ROLE.getLabel())) {
            roleType = Optional.empty();
        }

        // We can't use the shortcut edge in the presence of role-type hierarchies, because we don't know precisely
        // which type the shortcut edge label will have.
        if (roleType.isPresent() && hasDirectSubTypes(graph, roleType.get())) {
            return false;
        }

        fragmentSets.remove(castingFragmentSet);
        fragmentSets.remove(rolePlayerFragmentSet);
        castingIsaFragment.ifPresent(fragmentSets::remove);

        VarName rolePlayer = rolePlayerFragmentSet.rolePlayer();
        fragmentSets.add(new ShortcutFragmentSet(relation, casting, rolePlayer, roleType, relType));
        return true;
    }

    private static Optional<TypeLabel> findTypeLabel(Collection<EquivalentFragmentSet> fragmentSets, VarName type) {
        return fragmentSetOfType(LabelFragmentSet.class, fragmentSets)
                .filter(labelFragmentSet -> labelFragmentSet.type().equals(type))
                .map(LabelFragmentSet::label)
                .findAny();
    }

    private static RolePlayerFragmentSet findRolePlayerFragmentSet(
            Collection<EquivalentFragmentSet> fragmentSets, VarName casting) {
        // We can assume that this fragment set must exist
        //noinspection OptionalGetWithoutIsPresent
        return fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)
                .filter(rp -> rp.casting().equals(casting))
                .findAny()
                .get();
    }
}

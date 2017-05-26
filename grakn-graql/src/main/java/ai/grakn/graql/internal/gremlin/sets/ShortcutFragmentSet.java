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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static java.util.stream.Collectors.toSet;

/**
 * A query can use a shortcut edge traversal when the following criteria are met:
 *
 * <ol>
 *  <li>There is a {@link CastingFragmentSet} from {@code r} to {@code c}</li>
 *  <li>There is a {@link RolePlayerFragmentSet} from {@code c} to {@code x}</li>
 *  <li>If there is a {@link IsaFragmentSet} from {@code c} to {@code C}, then {@code C} must have a
 *  {@link LabelFragmentSet}</li>
 * </ol>
 *
 * The shortcut fragment can be constrained even further when any of the following optional criteria are met:
 *
 * <ol>
 *  <li>There is a {@link IsaFragmentSet} from {@code c} to a type with a {@link LabelFragmentSet}</li>
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

    ShortcutFragmentSet(VarProperty varProperty,
                        Var relation, Var edge, Var rolePlayer, Optional<Set<TypeLabel>> roleTypes,
            Optional<Set<TypeLabel>> relationTypes) {
        super(
                Fragments.inShortcut(varProperty, rolePlayer, edge, relation, roleTypes, relationTypes),
                Fragments.outShortcut(varProperty, relation, edge, rolePlayer, roleTypes, relationTypes)
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
        Var relation = castingFragmentSet.relation();
        Var casting = castingFragmentSet.casting();

        RolePlayerFragmentSet rolePlayerFragmentSet = findRolePlayerFragmentSet(fragmentSets, casting);

        // Try and get type of relation
        Optional<IsaFragmentSet> relIsaFragment = fragmentSetOfType(IsaFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.instance().equals(relation))
                .findAny();

        Optional<TypeLabel> relType = relIsaFragment
                .map(IsaFragmentSet::type)
                .flatMap(type -> findTypeLabel(fragmentSets, type));

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

        fragmentSets.remove(castingFragmentSet);
        fragmentSets.remove(rolePlayerFragmentSet);
        castingIsaFragment.ifPresent(fragmentSets::remove);

        Var rolePlayer = rolePlayerFragmentSet.rolePlayer();

        // Look up all sub-types
        Optional<Set<TypeLabel>> roleTypes = subTypes(graph, roleType);
        Optional<Set<TypeLabel>> relTypes = subTypes(graph, relType);

        fragmentSets.add(new ShortcutFragmentSet(null, relation, casting, rolePlayer, roleTypes, relTypes));
        return true;
    }

    private static Optional<Set<TypeLabel>> subTypes(GraknGraph graph, Optional<TypeLabel> type) {
        return type.map(label -> graph.getType(label).subTypes().stream().map(Type::getLabel).collect(toSet()));
    }

    private static Optional<TypeLabel> findTypeLabel(Collection<EquivalentFragmentSet> fragmentSets, Var type) {
        return fragmentSetOfType(LabelFragmentSet.class, fragmentSets)
                .filter(labelFragmentSet -> labelFragmentSet.type().equals(type))
                .map(LabelFragmentSet::label)
                .findAny();
    }

    private static RolePlayerFragmentSet findRolePlayerFragmentSet(
            Collection<EquivalentFragmentSet> fragmentSets, Var casting) {
        // We can assume that this fragment set must exist
        //noinspection OptionalGetWithoutIsPresent
        return fragmentSetOfType(RolePlayerFragmentSet.class, fragmentSets)
                .filter(rp -> rp.casting().equals(casting))
                .findAny()
                .get();
    }
}

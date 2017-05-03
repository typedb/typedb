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

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static java.util.stream.Collectors.toList;

/**
 * A query can use a shortcut edge traversal when the following criteria are met:
 *
 * <ol>
 *  <li>There is a {@link CastingFragmentSet} from {@code r} to {@code xc}</li>
 *  <li>There is a {@link CastingFragmentSet} from {@code r} to {@code yc}</li>
 *  <li>There is a {@link RolePlayerFragmentSet} from {@code xc} to {@code x}</li>
 *  <li>There is a {@link RolePlayerFragmentSet} from {@code yc} to {@code y}</li>
 *  <li>There is a {@link NeqFragmentSet} between {@code xc} and {@code yc}</li>
 *  <li>{@code r} does not have a user-defined variable name</li>
 * </ol>
 *
 * And optionally:
 *
 * <ol>
 *  <li>There is a {@link IsaFragmentSet} from {@code xc} to a type {@link VarName} with a {@link LabelFragmentSet}</li>
 *  <li>There is a {@link IsaFragmentSet} from {@code yc} to a type {@link VarName} with a {@link LabelFragmentSet}</li>
 *  <li>There is a {@link IsaFragmentSet} from {@code r} to a type {@link VarName} with a {@link LabelFragmentSet}</li>
 * </ol>
 *
 * We assume that {@code xc} and {@code yc} are otherwise never referred to in the query, since they are castings,
 * so the user cannot refer to them.
 *
 * When these criteria are met, all the fragments can be replaced with a {@link ShortcutFragmentSet} from {@code x}
 * to {@code y}, with optionally specified role- and relation-types.
 *
 * @author Felix Chapman
 */
class ShortcutFragmentSet extends EquivalentFragmentSet {

    ShortcutFragmentSet(
            Optional<TypeLabel> roleTypeA, VarName rolePlayerA,
            Optional<TypeLabel> roleTypeB, VarName rolePlayerB, Optional<TypeLabel> relationType) {
        super(
                Fragments.shortcut(relationType, roleTypeA, roleTypeB, rolePlayerA, rolePlayerB),
                Fragments.shortcut(relationType, roleTypeB, roleTypeA, rolePlayerB, rolePlayerA)
        );
    }

    static boolean applyShortcutOptimisation(Collection<EquivalentFragmentSet> fragmentSets) {
        Iterable<VarName> relations = fragmentSetOfType(CastingFragmentSet.class, fragmentSets)
                .map(CastingFragmentSet::relation)::iterator;

        for (VarName relation : relations) {
            if (attemptOptimiseRelation(fragmentSets, relation)) {
                return true;
            }
        }

        return false;
    }

    private static boolean attemptOptimiseRelation(Collection<EquivalentFragmentSet> fragmentSets, VarName relation) {
        List<CastingFragmentSet> castings = fragmentSetOfType(CastingFragmentSet.class, fragmentSets)
                .filter(casting -> casting.relation().equals(relation))
                .collect(toList());

        if (castings.size() != 2) return false;

        CastingFragmentSet castingFragmentX = castings.get(0);
        CastingFragmentSet castingFragmentY = castings.get(1);

        VarName castingX = castingFragmentX.casting();
        VarName castingY = castingFragmentY.casting();

        RolePlayerFragmentSet rolePlayerFragmentX = findRolePlayerFragmentSet(fragmentSets, castingX);
        RolePlayerFragmentSet rolePlayerFragmentY = findRolePlayerFragmentSet(fragmentSets, castingY);

        NeqFragmentSet distinctCasting = findNeqFragmentSet(fragmentSets, castingX, castingY);

        // Try and get type of relation
        Optional<IsaFragmentSet> relIsaFragment = fragmentSetOfType(IsaFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.instance().equals(relation))
                .findAny();

        Optional<TypeLabel> relType = relIsaFragment
                .map(IsaFragmentSet::type)
                .flatMap(type -> fragmentSetOfType(LabelFragmentSet.class, fragmentSets).filter(labelFragmentSet -> labelFragmentSet.type().equals(type)).map(LabelFragmentSet::label).findAny());

        if (relIsaFragment.isPresent() && !relType.isPresent()) {
            return false;
        }

        // Try and get role of X
        Optional<IsaCastingsFragmentSet> castingXIsaFragment = fragmentSetOfType(IsaCastingsFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.casting().equals(castingX))
                .findAny();

        Optional<TypeLabel> roleX = castingXIsaFragment
                .map(IsaCastingsFragmentSet::roleType)
                .flatMap(type -> fragmentSetOfType(LabelFragmentSet.class, fragmentSets).filter(labelFragmentSet -> labelFragmentSet.type().equals(type)).map(LabelFragmentSet::label).findAny());

        if (castingXIsaFragment.isPresent() && !roleX.isPresent()) {
            return false;
        }

        // Try and get role of Y
        Optional<IsaCastingsFragmentSet> castingYIsaFragment = fragmentSetOfType(IsaCastingsFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.casting().equals(castingY))
                .findAny();

        Optional<TypeLabel> roleY = castingYIsaFragment
                .map(IsaCastingsFragmentSet::roleType)
                .flatMap(type -> fragmentSetOfType(LabelFragmentSet.class, fragmentSets).filter(labelFragmentSet -> labelFragmentSet.type().equals(type)).map(LabelFragmentSet::label).findAny());

        if (castingYIsaFragment.isPresent() && !roleY.isPresent()) {
            return false;
        }

        fragmentSets.remove(castingFragmentX);
        fragmentSets.remove(castingFragmentY);
        fragmentSets.remove(rolePlayerFragmentX);
        fragmentSets.remove(rolePlayerFragmentY);
        fragmentSets.remove(distinctCasting);

        relIsaFragment.ifPresent(fragmentSets::remove);
        castingXIsaFragment.ifPresent(fragmentSets::remove);
        castingYIsaFragment.ifPresent(fragmentSets::remove);

        VarName rolePlayerX = rolePlayerFragmentX.rolePlayer();
        VarName rolePlayerY = rolePlayerFragmentY.rolePlayer();
        fragmentSets.add(new ShortcutFragmentSet(roleX, rolePlayerX, roleY, rolePlayerY, relType));
        return true;
    }

    private static NeqFragmentSet findNeqFragmentSet(
            Collection<EquivalentFragmentSet> fragmentSets, VarName varX, VarName varY) {
        // We can assume that this fragment set must exist
        //noinspection OptionalGetWithoutIsPresent
        return fragmentSetOfType(NeqFragmentSet.class, fragmentSets)
                .filter(neq -> neq.isBetween(varX, varY))
                .findAny()
                .get();
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

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.plan.GraqlTraversalPlanner.planFromTraversal;

/**
 * <p>
 *     Class containing old traversal planners which should be used for debugging purposes only
 * </p>
 */
@Deprecated
public class DebuggingPlanners {
    private static int basePriority = Integer.MAX_VALUE;

    /**
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using {@link GraqlTraversal}.
     */
    @SuppressWarnings("unused")
    static public ImmutableList<Atom> planSimple(ReasonerQueryImpl query){
        return ImmutableList.<Atom>builder().addAll(
                query.selectAtoms().stream()
                        .sorted(Comparator.comparing(at -> -baseResolutionPriority(at)))
                        .iterator())
                .build();
    }

    /**
     * @return measure of priority with which this atom should be resolved
     */
    @SuppressWarnings("unused")
    public static int baseResolutionPriority(Atom at){
        if (basePriority == Integer.MAX_VALUE) {
            basePriority = computePriority(at);
        }
        return basePriority;
    }

    /**
     * compute base resolution priority of this atom
     * @return priority value
     */
    @SuppressWarnings("unused")
    private static int computePriority(Atom at){
        return at.computePriority(at.getPartialSubstitutions().map(IdPredicate::getVarName).collect(Collectors.toSet()));
    }

    /**
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using {@link GraqlTraversal}.
     */
    @SuppressWarnings("unused")
    public static ImmutableList<Atom> plan(ReasonerQueryImpl query){
        List<Atom> atoms = query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toList());
        return planFromTraversal(atoms, query.getPattern(), query.tx());
    }
}

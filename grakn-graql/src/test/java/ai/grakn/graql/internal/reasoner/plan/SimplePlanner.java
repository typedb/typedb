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

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.plan.priority.AtomPriority;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;

/**
 *
 * <p>
 * Old simple resolution planner using predefined weights for different {@link Atom} configurations.}.
 * Used for debugging purposes.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@Deprecated
public class SimplePlanner {

    /**
     * priority modifier for each partial substitution a given atom has
     */
    public static final int PARTIAL_SUBSTITUTION = 30;

    /**
     * priority modifier if a given atom is a resource atom
     */
    public static final int IS_RESOURCE_ATOM = 0;

    /**
     * priority modifier if a given atom is a resource atom attached to a relation
     */
    public static final int RESOURCE_REIFYING_RELATION = 20;

    /**
     * priority modifier if a given atom is a type atom
     */
    public static final int IS_TYPE_ATOM = 0;

    /**
     * priority modifier if a given atom is a relation atom
     */
    public static final int IS_RELATION_ATOM = 2;

    /**
     * priority modifier if a given atom is a type atom without specific type
     * NB: atom satisfying this criterion should be resolved last
     */
    public static final int NON_SPECIFIC_TYPE_ATOM = -1000;


    public static final int RULE_RESOLVABLE_ATOM = -10;

    /**
     * priority modifier if a given atom is recursive atom
     */
    public static final int RECURSIVE_ATOM = -5;

    /**
     * priority modifier for guard (type atom) the atom has
     */
    public static final int GUARD = 1;

    /**
     * priority modifier for guard (type atom) the atom has - favour boundary rather than bulk atoms
     */
    public static final int BOUND_VARIABLE = -2;

    /**
     * priority modifier if an atom has an inequality predicate
     */
    public static final int INEQUALITY_PREDICATE = -1000;

    /**
     * priority modifier for each specific value predicate a given atom (resource) has
     */
    public static final int SPECIFIC_VALUE_PREDICATE = 20;

    /**
     * priority modifier for each non-specific value predicate a given atom (resource) has
     */
    public static final int NON_SPECIFIC_VALUE_PREDICATE = 5;

    /**
     * priority modifier for each value predicate with variable
     */
    public static final int VARIABLE_VALUE_PREDICATE = -100;

    /**
     * number of entities that need to be attached to a resource wtih a specific value to be considered a supernode
     */
    public static final int RESOURCE_SUPERNODE_SIZE = 5;

    /**
     * priority modifier for each value predicate with variable requiring comparison
     * NB: atom satisfying this criterion should be resolved last
     */
    public static final int COMPARISON_VARIABLE_VALUE_PREDICATE = - 1000;

    /**
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using {@link GraqlTraversal}.
     */
     public static ImmutableList<Atom> plan(ReasonerQueryImpl query){
        return ImmutableList.<Atom>builder().addAll(
                query.selectAtoms().stream()
                        .sorted(Comparator.comparing(at -> -AtomPriority.priority(at)))
                        .iterator())
                .build();
    }
}

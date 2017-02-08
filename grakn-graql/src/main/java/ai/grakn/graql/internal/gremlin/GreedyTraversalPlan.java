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

package ai.grakn.graql.internal.gremlin;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toSet;

/**
 * Class for generating greedy traversal plans
 *
 * @author Felix Chapman
 */
public class GreedyTraversalPlan {

    private static final long MAX_TRAVERSAL_ATTEMPTS = 1_000;

    /**
     * Create a semi-optimal traversal plan using a greedy approach
     * <p>
     * We can interpret building a plan as a decision tree, where each branch represents selecting a fragment to
     * traverse next. A path from the root to a leaf of the tree represents a traversal plan.
     * <p>
     * Under this definition, this traversal optimisation plan takes a brute-force approach. It traverses a certain
     * number of branches (defined in MAX_TRAVERSAL_ATTEMPTS) and calculates the estimated complexity of each partial
     * plan produced. The lowest complexity partial plan is chosen, then the brute-force search is re-started from that
     * position until a full plan is produced.
     * <p>
     * With fixed MAX_TRAVERSAL_ATTEMPTS, this method is O(n) where n is the size of the query. In general, it produces
     * optimal or nearly-optimal results, so a 'smarter' method may not be necessary.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal createTraversal(PatternAdmin pattern) {
        Collection<Conjunction<VarAdmin>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        // Find a semi-optimal way to execute each conjunction
        Set<? extends List<Fragment>> fragments = patterns.stream()
                .map(ConjunctionQuery::new)
                .map(GreedyTraversalPlan::semiOptimalConjunction)
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a semi-optimal plan using a greedy approach to execute a single conjunction
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> semiOptimalConjunction(ConjunctionQuery query) {

        Set<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(query.getEquivalentFragmentSets());
        Set<VarName> names = new HashSet<>();

        long numFragments = fragments(fragmentSets).count();
        long depth = 1;
        long numTraversalAttempts = numFragments;

        // Calculate the depth to descend in the tree, based on how many plans we want to evaluate
        while (numFragments > 0 && numTraversalAttempts < MAX_TRAVERSAL_ATTEMPTS) {
            depth += 1;
            numTraversalAttempts *= numFragments;
            numFragments -= 1;
        }

        Plan plan = Plan.base();

        while (!fragmentSets.isEmpty()) {
            plan = extendPlan(plan, fragmentSets, names, depth);
            List<Fragment> newFragments = plan.fragments();

            if (newFragments.isEmpty()) {
                throw new RuntimeException(ErrorMessage.FAILED_TO_BUILD_TRAVERSAL.getMessage());
            }

            newFragments.forEach(fragment -> {
                fragmentSets.remove(fragment.getEquivalentFragmentSet());
                fragment.getVariableNames().forEach(names::add);
            });
        }

        return plan.fragments();
    }

    /**
     * Find a traversal plan that will satisfy the given equivalent fragment sets
     * @param plan the plan so far
     * @param fragmentSets a set of equivalent fragment sets that must all be covered by the plan
     * @param names a set of names that have already been encountered while executing the query
     * @param depth the maximum depth the plan is allowed to descend in the tree
     * @return a new plan that extends the given plan
     */
    private static Plan extendPlan(Plan plan, Set<EquivalentFragmentSet> fragmentSets, Set<VarName> names, long depth) {

        // Base case
        if (depth == 0) return plan;

        // A function that will recursively extend the plan using the given fragment
        Function<Fragment, Plan> extendPlanWithFragment = fragment -> {
            // Create the new plan, fragment sets and variable names when using this fragment
            Plan newPlan = plan.append(fragment, names);
            EquivalentFragmentSet fragmentSet = fragment.getEquivalentFragmentSet();
            Set<EquivalentFragmentSet> newFragmentSets = Sets.difference(fragmentSets, ImmutableSet.of(fragmentSet));
            Set<VarName> newNames = Sets.union(names, fragment.getVariableNames().collect(toSet()));

            // Recursively find a plan
            return extendPlan(newPlan, newFragmentSets, newNames, depth - 1);
        };

        // Create a plan for every fragment that has its dependencies met, then select the lowest cost plan
        return fragments(fragmentSets)
                .filter(fragment -> names.containsAll(fragment.getDependencies()))
                .map(extendPlanWithFragment)
                .min(naturalOrder())
                .orElse(plan);
    }

    private static Stream<Fragment> fragments(Set<EquivalentFragmentSet> fragmentSets) {
        return fragmentSets.stream().flatMap(EquivalentFragmentSet::getFragments);
    }
}

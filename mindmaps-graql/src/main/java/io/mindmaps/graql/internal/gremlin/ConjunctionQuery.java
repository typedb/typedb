/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.util.ErrorMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

/**
 * A query that does not contain any disjunctions, so it can be represented as a single gremlin traversal.
 * <p>
 * The {@code ConjunctionQuery} is passed a {@code Pattern.Conjunction<Var.Admin>}. A {@code VarTraversal} can be
 * extracted from each {@code Var} and {@code EquivalentFragmentSet}s can be extracted from each {@code VarTraversal}.
 * <p>
 * The {@code EquivalentFragmentSet}s are sorted to produce a set of lists of {@code Fragments}. Each list of fragments
 * describes a connected component in the query. Most queries are completely connected, so there will be only one
 * list of fragments in the set. If the query is disconnected (e.g. match $x isa movie, $y isa person), then there
 * will be multiple lists of fragments in the set.
 * <p>
 * A gremlin traversal is created by concatenating the traversals within each fragment.
 */
class ConjunctionQuery {

    private final Set<VarAdmin> vars;
    private final ImmutableSet<EquivalentFragmentSet> equivalentFragmentSets;
    private final ImmutableList<Fragment> sortedFragments;

    /**
     * @param patternConjunction a pattern containing no disjunctions to find in the graph
     */
    ConjunctionQuery(Conjunction<VarAdmin> patternConjunction) {
        vars = patternConjunction.getPatterns();

        if (vars.size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.equivalentFragmentSets =
                vars.stream().flatMap(Traversals::equivalentFragmentSets).collect(toImmutableSet());

        this.sortedFragments = sortFragments();
    }

    /**
     * @return a stream of concept IDs mentioned in the query
     */
    Stream<String> getConcepts() {
        return vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeIds().stream());
    }

    ImmutableList<Fragment> getSortedFragments() {
        return sortedFragments;
    }

    /**
     * Sort the fragments describing the query, such that every property is represented in the fragments and the
     * fragments are ordered by priority in order to perform the query quickly.
     *
     * There will be one list of fragments for every connected component of the query.
     *
     * @return a set of list of fragments, sorted by priority.
     */
    private ImmutableList<Fragment> sortFragments() {
        // Sort fragments using a topological sort, such that each fragment leads to the next.
        // fragments are also sorted by "priority" to improve the performance of the search

        // Maintain a map of fragments grouped by starting variable for fast lookup
        Map<String, Set<Fragment>> fragmentMap = streamFragments().collect(groupingBy(Fragment::getStart, toSet()));

        // Track properties and fragments that have been used
        Set<Fragment> remainingFragments = streamFragments().collect(toSet());
        Set<EquivalentFragmentSet> remainingTraversals = Sets.newHashSet(equivalentFragmentSets);
        Set<EquivalentFragmentSet> matchedTraversals = new HashSet<>();

        // Result set of fragments (one entry in the set for each connected part of the query)
        List<Fragment> sortedFragments = new ArrayList<>();

        while (!remainingTraversals.isEmpty()) {
            // Traversal is started from the highest priority fragment
            Optional<Fragment> optionalFragment = remainingFragments.stream().min(naturalOrder());
            Fragment highestFragment = optionalFragment.orElseThrow(
                    () -> new RuntimeException(ErrorMessage.FAILED_TO_BUILD_TRAVERSAL.getMessage())
            );
            String start = highestFragment.getStart();

            // A queue of reachable fragments, with the highest priority fragments always on top
            PriorityQueue<Fragment> reachableFragments = new PriorityQueue<>(fragmentMap.get(start));

            while (!reachableFragments.isEmpty()) {
                // Take highest priority fragment from reachable fragments
                Fragment fragment = reachableFragments.poll();
                EquivalentFragmentSet equivalentFragmentSet = fragment.getEquivalentFragmentSet();

                // Only choose one fragment from each pattern
                if (matchedTraversals.contains(equivalentFragmentSet)) continue;

                remainingFragments.remove(fragment);
                remainingTraversals.remove(equivalentFragmentSet);
                matchedTraversals.add(equivalentFragmentSet);
                sortedFragments.add(fragment);

                // If the fragment has a variable at the end, then fragments starting at that variable are reachable
                fragment.getEnd().ifPresent(
                        end -> {
                            Set<Fragment> fragments = fragmentMap.remove(end);
                            if (fragments != null) reachableFragments.addAll(fragments);
                        }
                );
            }
        }

        return ImmutableList.copyOf(sortedFragments);
    }

    /**
     * @return a stream of Fragments in this query
     */
    private Stream<Fragment> streamFragments() {
        return equivalentFragmentSets.stream().flatMap(EquivalentFragmentSet::getFragments);
    }
}

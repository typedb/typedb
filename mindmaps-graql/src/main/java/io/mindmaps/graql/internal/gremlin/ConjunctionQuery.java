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

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

/**
 * A query that does not contain any disjunctions, so it can be represented as a single gremlin traversal.
 * <p>
 * The {@code ConjunctionQuery} is passed a {@code Pattern.Conjunction<Var.Admin>}. A {@code VarTraversal} can be
 * extracted from each {@code Var} and {@code MultiTraversals} can be extracted from each {@code VarTraversal}.
 * <p>
 * The {@code MultiTraversals} are sorted to produce a set of lists of {@code Fragments}. Each list of fragments
 * describes a connected component in the query. Most queries are completely connected, so there will be only one
 * list of fragments in the set. If the query is disconnected (e.g. match $x isa movie, $y isa person), then there
 * will be multiple lists of fragments in the set.
 * <p>
 * A gremlin traversal is created by concatenating the traversals within each fragment.
 */
class ConjunctionQuery {

    private final Set<Var.Admin> vars;
    private final Set<List<Fragment>> fragments;
    private final MindmapsTransaction transaction;

    /**
     * @param patternConjunction a pattern containing no disjunctions to find in the graph
     */
    ConjunctionQuery(MindmapsTransaction transaction, Pattern.Conjunction<Var.Admin> patternConjunction) {
        this.transaction = transaction;
        vars = patternConjunction.getPatterns();

        if (vars.size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.fragments = sortedFragments();

    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    GraphTraversal<Vertex, Map<String, Vertex>> getTraversal() {
        GraphTraversal<Vertex, Vertex> traversal =
                ((MindmapsTransactionImpl) transaction).getTinkerPopGraph().traversal().V();

        Set<String> foundNames = new HashSet<>();

        Iterator<List<Fragment>> fragmentIterator = fragments.iterator();

        // Apply fragments in order into one single traversal
        while (fragmentIterator.hasNext()) {
            String currentName = null;
            List<Fragment> fragmentList = fragmentIterator.next();

            for (Fragment fragment : fragmentList) {
                applyFragment(fragment, traversal, currentName, foundNames);
                currentName = fragment.getEnd().orElse(fragment.getStart());
            }

            // Restart traversal for each connected component
            if (fragmentIterator.hasNext()) traversal = traversal.V();
        }

        // Select all the variable names
        String[] traversalNames = foundNames.toArray(new String[foundNames.size()]);
        return traversal.select(traversalNames[0], traversalNames[0], traversalNames);
    }

    /**
     * Apply the given fragment to the traversal. Keeps track of variable names so far so that it can decide whether
     * to use "as" or "select" steps in gremlin.
     * @param fragment the fragment to apply to the traversal
     * @param traversal the gremlin traversal to apply the fragment to
     * @param currentName the variable name that the traversal is currently at
     * @param names a set of variable names so far encountered in the query
     */
    private void applyFragment(
            Fragment fragment, GraphTraversal<Vertex, Vertex> traversal, String currentName, Set<String> names
    ) {
        String start = fragment.getStart();

        if (currentName != null) {
            if (!currentName.equals(start)) {
                // If the variable name has been visited but the traversal is not at that variable name, select it
                traversal.select(start);
            }
        } else {
            // If the variable name has not been visited yet, remember it and use the 'as' step
            names.add(start);
            traversal.as(start);
        }

        // Apply fragment to traversal
        fragment.applyTraversal(traversal);

        fragment.getEnd().ifPresent(end -> {
            if (!names.contains(end)) {
                // This variable name has not been encountered before, remember it and use the 'as' step
                names.add(end);
                traversal.as(end);
            } else {
                // This variable name has been encountered before, confirm it is the same
                traversal.where(P.eq(end));
            }
        });
    }

    /**
     * @return a stream of concept IDs mentioned in the query
     */
    Stream<String> getConcepts() {
        return vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeIds().stream());
    }

    /**
     * Sort the fragments describing the query, such that every property is represented in the fragments and the
     * fragments are ordered by priority in order to perform the query quickly.
     *
     * There will be one list of fragments for every connected component of the query.
     *
     * @return a set of list of fragments, sorted by priority.
     */
    private Set<List<Fragment>> sortedFragments() {
        // Sort fragments using a topological sort, such that each fragment leads to the next.
        // fragments are also sorted by "priority" to improve the performance of the search

        // Maintain a map of fragments grouped by starting variable for fast lookup
        Map<String, Set<Fragment>> fragmentMap = getFragments().collect(groupingBy(Fragment::getStart, toSet()));

        // Track properties and fragments that have been used
        Set<Fragment> remainingFragments = getFragments().collect(toSet());
        Set<MultiTraversal> remainingTraversals = getMultiTraversals().collect(toSet());
        Set<MultiTraversal> matchedTraversals = new HashSet<>();

        // Result set of fragments (one entry in the set for each connected part of the query)
        Set<List<Fragment>> allSortedFragments = new HashSet<>();

        while (!remainingTraversals.isEmpty()) {
            // Traversal is started from the highest priority fragment
            Fragment highestFragment = remainingFragments.stream().min(naturalOrder()).get();
            String start = highestFragment.getStart();

            // A queue of reachable fragments, with the highest priority fragments always on top
            PriorityQueue<Fragment> reachableFragments = new PriorityQueue<>(fragmentMap.get(start));

            List<Fragment> sortedFragments = new ArrayList<>();

            while (!reachableFragments.isEmpty()) {
                // Take highest priority fragment from reachable fragments
                Fragment fragment = reachableFragments.poll();
                MultiTraversal multiTraversal = fragment.getMultiTraversal();

                // Only choose one fragment from each pattern
                if (matchedTraversals.contains(multiTraversal)) continue;

                remainingFragments.remove(fragment);
                remainingTraversals.remove(multiTraversal);
                matchedTraversals.add(multiTraversal);
                sortedFragments.add(fragment);

                // If the fragment has a variable at the end, then fragments starting at that variable are reachable
                fragment.getEnd().ifPresent(
                        end -> {
                            Set<Fragment> fragments = fragmentMap.remove(end);
                            if (fragments != null) reachableFragments.addAll(fragments);
                        }
                );
            }

            allSortedFragments.add(sortedFragments);
        }

        return allSortedFragments;
    }

    /**
     * @return a stream of MultiTraversals in this query
     */
    private Stream<MultiTraversal> getMultiTraversals() {
        return vars.stream().flatMap(var -> var.getMultiTraversals().stream());
    }

    /**
     * @return a stream of Fragments in this query
     */
    private Stream<Fragment> getFragments() {
        return getMultiTraversals().flatMap(MultiTraversal::getFragments);
    }
}

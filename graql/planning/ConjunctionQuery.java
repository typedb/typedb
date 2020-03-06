/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.planning;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.graql.planning.gremlin.fragment.NeqFragment;
import grakn.core.graql.planning.gremlin.fragment.ValueFragment;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.exception.GraqlException;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A query that does not contain any disjunctions, so it can be represented as a single gremlin traversal.
 * The {@code ConjunctionQuery} is passed a Conjunction<Statement>.
 * EquivalentFragmentSets can be extracted from each GraqlTraversal.
 * The EquivalentFragmentSets are sorted to produce a set of lists of Fragments. Each list of fragments
 * describes a connected component in the query. Most queries are completely connected, so there will be only one
 * list of fragments in the set. If the query is disconnected (e.g. match $x isa movie, $y isa person), then there
 * will be multiple lists of fragments in the set.
 *
 * A gremlin traversal is created by concatenating the traversals within each fragment.
 */
class ConjunctionQuery {

    private final Set<Statement> statements;
    private PropertyExecutorFactory propertyExecutorFactory;

    private final ImmutableSet<EquivalentFragmentSet> equivalentFragmentSets;

    /**
     * @param patternConjunction a pattern containing no disjunctions to find in the graph
     */
    ConjunctionQuery(Conjunction<Statement> patternConjunction, ConceptManager conceptManager, PropertyExecutorFactory propertyExecutorFactory) {
        statements = patternConjunction.getPatterns();
        this.propertyExecutorFactory = propertyExecutorFactory;

        if (statements.size() == 0) {
            throw GraqlException.noPatterns();
        }

        ImmutableSet<EquivalentFragmentSet> fragmentSets =
                statements.stream().flatMap(statements -> equivalentFragmentSetsRecursive(statements)).collect(ImmutableSet.toImmutableSet());

        // Get all variable names mentioned in non-starting, non-comparing fragments (these should have vars bound elsewhere too)
        Set<Variable> names = fragmentSets.stream()
                .flatMap(EquivalentFragmentSet::stream)
                .filter(fragment -> !fragment.validStartIfDisconnected()
                                && !(fragment instanceof ValueFragment)
                                && !(fragment instanceof NeqFragment))
                .flatMap(fragment -> fragment.vars().stream())
                .collect(ImmutableSet.toImmutableSet());

        // Get all dependencies fragments have on certain variables existing
        Set<Variable> dependencies = fragmentSets.stream()
                .flatMap(EquivalentFragmentSet::stream)
                .flatMap(fragment -> fragment.dependencies().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<Variable> validNames = Sets.difference(names, dependencies);

        // Filter out any non-essential starting fragments (because other fragments refer to their starting variable)
        Set<EquivalentFragmentSet> initialEquivalentFragmentSets = fragmentSets.stream()
                .filter(set -> set.stream().anyMatch(
                        fragment -> !fragment.validStartIfDisconnected() || !validNames.contains(fragment.start())
                ))
                .collect(toSet());

        // Apply final optimisations
        EquivalentFragmentSets.optimiseFragmentSets(initialEquivalentFragmentSets, conceptManager);

        this.equivalentFragmentSets = ImmutableSet.copyOf(initialEquivalentFragmentSets);
    }

    ImmutableSet<EquivalentFragmentSet> getEquivalentFragmentSets() {
        return equivalentFragmentSets;
    }

    /**
     * Get all possible orderings of fragments
     */
    Set<List<Fragment>> allFragmentOrders() {
        Collection<List<EquivalentFragmentSet>> fragmentSetPermutations = Collections2.permutations(equivalentFragmentSets);
        return fragmentSetPermutations.stream().flatMap(this::cartesianProduct).collect(toSet());
    }

    private Stream<List<Fragment>> cartesianProduct(List<EquivalentFragmentSet> fragmentSets) {
        // Get fragments in each set
        List<Set<Fragment>> fragments = fragmentSets.stream()
                .map(EquivalentFragmentSet::fragments)
                .collect(toList());
        return Sets.cartesianProduct(fragments).stream();
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSetsRecursive(Statement statement) {
        return statement.innerStatements().stream().flatMap(s -> equivalentFragmentSets(s));
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSets(Statement statement) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        Variable start = statement.var();

        statement.properties().stream().forEach(property -> {
            Collection<EquivalentFragmentSet> newTraversals = propertyExecutorFactory.create(start, property).matchFragments();
            traversals.addAll(newTraversals);
        });

        if (!traversals.isEmpty()) {
            return traversals.stream();
        } else {
            // If this variable has no properties, only confirm that it is not internal and nothing else.
            return Stream.of(EquivalentFragmentSets.notInternalFragmentSet(null, start));
        }
    }
}

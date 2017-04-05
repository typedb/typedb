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
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;
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

    /**
     * @param patternConjunction a pattern containing no disjunctions to find in the graph
     */
    ConjunctionQuery(Conjunction<VarAdmin> patternConjunction, GraknGraph graph) {
        vars = patternConjunction.getPatterns();

        if (vars.size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.NO_PATTERNS.getMessage());
        }

        ImmutableSet<EquivalentFragmentSet> fragmentSets =
                vars.stream().flatMap(ConjunctionQuery::equivalentFragmentSetsRecursive).collect(toImmutableSet());

        // Get all variable names mentioned in non-starting fragments
        Set<VarName> names = fragmentSets.stream()
                .flatMap(EquivalentFragmentSet::stream)
                .filter(fragment -> !fragment.isStartingFragment())
                .flatMap(fragment -> fragment.getVariableNames().stream())
                .collect(toImmutableSet());

        // Get all dependencies fragments have on certain variables existing
        Set<VarName> dependencies = fragmentSets.stream()
                .flatMap(EquivalentFragmentSet::stream)
                .flatMap(fragment -> fragment.getDependencies().stream())
                .collect(toImmutableSet());

        Set<VarName> validNames = Sets.difference(names, dependencies);

        // Filter out any non-essential starting fragments (because other fragments refer to their starting variable)
        Set<EquivalentFragmentSet> initialEquivalentFragmentSets = fragmentSets.stream()
                .filter(set -> set.stream().anyMatch(
                        fragment -> !fragment.isStartingFragment() || !validNames.contains(fragment.getStart())
                ))
                .collect(toSet());

        // Apply final optimisations
        EquivalentFragmentSets.optimiseFragmentSets(initialEquivalentFragmentSets, graph);

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
        return fragmentSetPermutations.stream().flatMap(ConjunctionQuery::cartesianProduct).collect(toSet());
    }

    private static Stream<List<Fragment>> cartesianProduct(List<EquivalentFragmentSet> fragmentSets) {
        // Get fragments in each set
        List<Set<Fragment>> fragments = fragmentSets.stream()
                .map(EquivalentFragmentSet::fragments)
                .collect(toList());
        return Sets.cartesianProduct(fragments).stream();
    }

    /**
     * @return a stream of concept names mentioned in the query
     */
    Stream<TypeLabel> getTypes() {
        return vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeLabels().stream());
    }

    private static Stream<EquivalentFragmentSet> equivalentFragmentSetsRecursive(VarAdmin var) {
        return var.getImplicitInnerVars().stream().flatMap(ConjunctionQuery::equivalentFragmentSetsOfVar);
    }

    private static Stream<EquivalentFragmentSet> equivalentFragmentSetsOfVar(VarAdmin var) {
        ShortcutTraversal shortcutTraversal = new ShortcutTraversal();
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        // If the user has provided a variable name, it can't be represented with a shortcut edge because it may be
        // referred to later.
        if (var.isUserDefinedName()) {
            shortcutTraversal.setInvalid();
        }

        VarName start = var.getVarName();

        var.getProperties().forEach(property -> {
            VarPropertyInternal propertyInternal = (VarPropertyInternal) property;
            propertyInternal.modifyShortcutTraversal(shortcutTraversal);
            Collection<EquivalentFragmentSet> newTraversals = propertyInternal.match(start);
            traversals.addAll(newTraversals);
        });

        if (shortcutTraversal.isValid()) {
            return Stream.of(shortcutTraversal.getEquivalentFragmentSet());
        } else if (!traversals.isEmpty()) {
            return traversals.stream();
        } else {
            // If this variable has no properties, only confirm that it is not a casting and nothing else.
            return Stream.of(EquivalentFragmentSets.notCasting(start));
        }
    }

}

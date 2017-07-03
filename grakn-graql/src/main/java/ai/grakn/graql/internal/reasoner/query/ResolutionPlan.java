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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Class defining the resolution plan in terms of different weights applicable to certain {@link Atom} configurations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public final class ResolutionPlan {

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
    public static final int RESOURCE_REIFIYING_RELATION = 20;

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
     * priority modifier for each value predicate with variable requiring comparison
     * NB: atom satisfying this criterion should be resolved last
     */
    public static final int COMPARISON_VARIABLE_VALUE_PREDICATE = - 1000;

    /**
     * compute the resolution plan - list of atomic queries ordered by their resolution priority
     * @return list of prioritised atomic queries
     */
    static LinkedList<ReasonerQueryImpl> getResolutionPlan(ReasonerQueryImpl query){
        LinkedList<ReasonerQueryImpl> queries = new LinkedList<>();
        GraknGraph graph = query.graph();

        LinkedList<Atom> atoms = query.selectAtoms().stream()
                .sorted(Comparator.comparing(at -> -at.baseResolutionPriority()))
                .collect(Collectors.toCollection(LinkedList::new));

        //TODO
        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(query.getPattern(), graph);
        ImmutableList<Fragment> fragments = graqlTraversal.fragments().iterator().next();

        LinkedHashSet<VarProperty> properties = fragments.stream()
                .map(Fragment::getVarProperty)
                .filter(Objects::nonNull)
                .filter(p -> !(p instanceof IdProperty))
                .filter(p -> !(p instanceof IsaProperty))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Atom top = atoms.getFirst();
        Set<Atom> nonResolvableAtoms = new HashSet<>();
        Set<Var> subbedVars = query.getIdPredicates().stream().map(IdPredicate::getVarName).collect(Collectors.toSet());
        while (!atoms.isEmpty()) {

            subbedVars.addAll(top.getVarNames());
            atoms.remove(top);

            if (top.isRuleResolvable()) {
                if (!nonResolvableAtoms.isEmpty()){
                    queries.add(ReasonerQueries.create(nonResolvableAtoms, graph));
                    nonResolvableAtoms.clear();
                }
                queries.add(new ReasonerAtomicQuery(top));
            } else {
                nonResolvableAtoms.add(top);
                if (atoms.isEmpty()) queries.add(ReasonerQueries.create(nonResolvableAtoms, graph));
            }

            //look at neighbours up to two hops away
            top = top.getNeighbours().filter(atoms::contains)
                    .flatMap(at -> Stream.concat(Stream.of(at), at.getNeighbours().filter(atoms::contains)))
                    .sorted(Comparator.comparing(at -> -at.computePriority(subbedVars)))
                    .findFirst().orElse(null);

            //top is disconnected atom
            if (top == null) {
                top = atoms.stream()
                        .sorted(Comparator.comparing(at -> -at.computePriority(subbedVars)))
                        .findFirst().orElse(null);
            }
        }

        return queries;
    }
}


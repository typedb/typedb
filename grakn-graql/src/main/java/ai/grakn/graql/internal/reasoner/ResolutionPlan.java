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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl}.
 * The plan is constructed either using the {@link GraqlTraversal} or in terms of different weights applicable to certain {@link Atom} configurations.
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

    final private ImmutableList<Atom> plan;
    final private GraknTx tx;

    public ResolutionPlan(ReasonerQueryImpl query){
        this.tx =  query.tx();
        this.plan = planFromTraversal(query);
        if (!isValid()) {
            throw GraqlQueryException.nonGroundNeqPredicate(query);
        }
    }

    @Override
    public String toString(){
        return plan.stream().map(AtomicBase::toString).collect(Collectors.joining("\n"));
    }

    /**
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using {@link GraqlTraversal}.
     */
    private ImmutableList<Atom> planFromTraversal(ReasonerQueryImpl query){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .forEach(at -> at.getVarProperties().forEach(p -> propertyMap.put(p, at)));
        Set<VarProperty> properties = propertyMap.keySet();

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(query.getPattern(), tx);
        ImmutableList<Fragment> fragments = graqlTraversal.fragments().iterator().next();

        return ImmutableList.<Atom>builder().addAll(
                fragments.stream()
                        .map(Fragment::varProperty)
                        .filter(Objects::nonNull)
                        .filter(properties::contains)
                        .distinct()
                        .flatMap(p -> propertyMap.get(p).stream())
                        .distinct()
                        .iterator())
                .build();
    }

    /**
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using {@link GraqlTraversal}.
     */
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private ImmutableList<Atom> plan(ReasonerQueryImpl query){
        return ImmutableList.<Atom>builder().addAll(
                query.selectAtoms().stream()
                        .sorted(Comparator.comparing(at -> -at.baseResolutionPriority()))
                        .iterator())
                .build();
    }

    /**
     * @return true if the plan doesn't lead to any non-ground neq predicate
     */
    private boolean isValid() {
        //check for neq groundness
        Set<NeqPredicate> nonGroundPredicates = new HashSet<>();
        Set<Var> mappedVars = new HashSet<>();
        for(Atom atom : plan){
            mappedVars.addAll(atom.getVarNames());
            atom.getPredicates(NeqPredicate.class)
                    .forEach(neq -> {
                        //look for non-local non-ground predicates
                        if (!mappedVars.containsAll(neq.getVarNames())
                                && !atom.getVarNames().containsAll(neq.getVarNames())){
                            nonGroundPredicates.add(neq);
                        } else{
                            //if this is ground for this atom but non-ground for another it is ground
                            if (nonGroundPredicates.contains(neq)) nonGroundPredicates.remove(neq);
                        }
                    });
        }
        return nonGroundPredicates.isEmpty();
    }

    /**
     * compute the query resolution plan - list of queries ordered by their cost as computed by the graql traversal planner
     * @return list of prioritised queries
     */
    public LinkedList<ReasonerQueryImpl> queryPlan(){
        LinkedList<ReasonerQueryImpl> queries = new LinkedList<>();
        LinkedList<Atom> atoms = new LinkedList<>(plan);

        List<Atom> nonResolvableAtoms = new ArrayList<>();
        while (!atoms.isEmpty()) {
            Atom top = atoms.remove();
            if (top.isRuleResolvable()) {
                if (!nonResolvableAtoms.isEmpty()) {
                    queries.add(ReasonerQueries.create(nonResolvableAtoms, tx));
                    nonResolvableAtoms.clear();
                }
                queries.add(ReasonerQueries.atomic(top));
            } else {
                nonResolvableAtoms.add(top);
                if (atoms.isEmpty()) queries.add(ReasonerQueries.create(nonResolvableAtoms, tx));
            }
        }
        return queries;
    }

    /**
     * compute the local query resolution plan - list of queries ordered by their resolution priority
     * @return list of prioritised queries
     */
    public LinkedList<ReasonerQueryImpl> localQueryPlan(){
        LinkedList<ReasonerQueryImpl> queries = new LinkedList<>();
        LinkedList<Atom> atoms = new LinkedList<>(plan);

        Atom top = atoms.getFirst();
        List<Atom> nonResolvableAtoms = new ArrayList<>();
        Set<Var> subbedVars = top.getParentQuery().getAtoms(IdPredicate.class).map(IdPredicate::getVarName).collect(Collectors.toSet());

        while (!atoms.isEmpty()) {

            subbedVars.addAll(top.getVarNames());
            atoms.remove(top);

            if (top.isRuleResolvable()) {
                if (!nonResolvableAtoms.isEmpty()){
                    queries.add(ReasonerQueries.create(nonResolvableAtoms, tx));
                    nonResolvableAtoms.clear();
                }
                queries.add(ReasonerQueries.atomic(top));
            } else {
                nonResolvableAtoms.add(top);
                if (atoms.isEmpty()) queries.add(ReasonerQueries.create(nonResolvableAtoms, tx));
            }

            //look at neighbours up to two hops away
            top = top.getNeighbours(Atom.class).filter(atoms::contains)
                    .flatMap(at -> Stream.concat(Stream.of(at), at.getNeighbours(Atom.class).filter(atoms::contains)))
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


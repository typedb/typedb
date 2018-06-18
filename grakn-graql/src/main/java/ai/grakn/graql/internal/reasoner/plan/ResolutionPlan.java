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

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl}.
 * The plan is constructed  using the {@link GraqlTraversal} with the aid of {@link GraqlTraversalPlanner}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public final class ResolutionPlan {

    final private ImmutableList<Atom> plan;
    final private EmbeddedGraknTx<?> tx;

    public ResolutionPlan(ReasonerQueryImpl query){
        this.tx =  query.tx();
        this.plan = GraqlTraversalPlanner.plan(query);
        if (!isValid()) {
            throw GraqlQueryException.nonGroundNeqPredicate(query);
        }
    }

    @Override
    public String toString(){
        return plan.stream().map(AtomicBase::toString).collect(Collectors.joining("\n"));
    }

    /**
     * @return corresponding atom plan
     */
    public ImmutableList<Atom> plan(){ return plan;}

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
        List<ReasonerQueryImpl> queries = new LinkedList<>();
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
        return refinedQueryPlan(queries);
    }

    private List<ReasonerQueryImpl> prioritiseQueries(List<ReasonerQueryImpl> queries){
        return queries.stream()
                .sorted(Comparator.comparing(q -> !q.isAtomic()))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isRuleResolvable))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isDisconnected))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private LinkedList<ReasonerQueryImpl> refinedQueryPlan(List<ReasonerQueryImpl> queries){
        Equivalence<ReasonerQuery> equality = ReasonerQueryEquivalence.Equality;

        LinkedList<ReasonerQueryImpl> plan = new LinkedList<>();
        Stack<ReasonerQueryImpl> queryStack = new Stack<>();

        Multimap<Equivalence.Wrapper<ReasonerQueryImpl>, Equivalence.Wrapper<ReasonerQueryImpl>> neighbourMap = HashMultimap.create();

        //determine connectivity
        queries.forEach(q -> {
                    Set<Var> vars = q.getVarNames();
                    Equivalence.Wrapper<ReasonerQueryImpl> wrappedQ = equality.wrap(q);
                    queries.stream()
                            .filter(q2 -> !equality.equivalent(q, q2))
                            .filter(q2 -> !Sets.intersection(vars, q2.getVarNames()).isEmpty())
                            .map(equality::wrap)
                            .forEach(q2 -> neighbourMap.put(wrappedQ, q2));
                });

        //prioritise queries
        Lists.reverse(prioritiseQueries(queries)).forEach(queryStack::push);
        while(!plan.containsAll(queries)) {
            ReasonerQueryImpl query = queryStack.pop();
            Equivalence.Wrapper<ReasonerQueryImpl> wrappedQuery = equality.wrap(query);

            //candidates
            List<ReasonerQueryImpl> candidates = prioritiseQueries(
                    neighbourMap.get(wrappedQuery).stream()
                            .map(Equivalence.Wrapper::get)
                            .filter(q -> !(plan.contains(q) || equality.equivalent(q, query)))
                            .collect(Collectors.toList())
            );

            if (!candidates.isEmpty() || queries.size() - plan.size() == 1){
                plan.add(query);
                candidates.forEach(queryStack::push);
            }
        }

        return plan;
    }
}


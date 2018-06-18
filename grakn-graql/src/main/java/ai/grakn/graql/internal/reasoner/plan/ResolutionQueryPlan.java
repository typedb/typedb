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

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.atom.Atom;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;


/**
 *
 * <p>
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl} at a query level.
 * The plan is constructed using the {@link ResolutionPlan} working at an atom level.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ResolutionQueryPlan {

    private final ImmutableList<ReasonerQueryImpl> queryPlan;

    public ResolutionQueryPlan(ReasonerQueryImpl query){
        this.queryPlan = queryPlan(query);
    }

    @Override
    public String toString(){
        return queries().stream()
                .map(sq -> sq.toString() + (sq.isRuleResolvable()? "*" : ""))
                .collect(Collectors.joining("\n"));
    }

    public List<ReasonerQueryImpl> queries(){ return queryPlan;}

    /**
     * compute the query resolution plan - list of queries ordered by their cost as computed by the graql traversal planner
     * @return list of prioritised queries
     */
    private static ImmutableList<ReasonerQueryImpl> queryPlan(ReasonerQueryImpl query){
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        EmbeddedGraknTx<?> tx = query.tx();
        LinkedList<Atom> atoms = new LinkedList<>(plan);
        List<ReasonerQueryImpl> queries = new LinkedList<>();

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
        return ImmutableList.copyOf(refinePlan(queries));
    }

    private static List<ReasonerQueryImpl> prioritiseQueries(List<ReasonerQueryImpl> queries){
        return queries.stream()
                .sorted(Comparator.comparing(q -> !q.isAtomic()))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isRuleResolvable))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isDisconnected))
                //.sorted(Comparator.comparing(q -> -q.getAtoms(IdPredicate.class).count()))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private static List<ReasonerQueryImpl> refinePlan(List<ReasonerQueryImpl> queries){
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
            if (queryStack.isEmpty()){
                System.out.println();
            }
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
                Lists.reverse(candidates).forEach(queryStack::push);
            }
        }

        return plan;
    }
}

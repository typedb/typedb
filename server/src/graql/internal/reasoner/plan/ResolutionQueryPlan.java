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

package grakn.core.graql.internal.reasoner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.server.session.TransactionOLTP;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;


/**
 *
 * <p>
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl} at a query level.
 * The plan is constructed using the {@link ResolutionPlan} working at an atom level.
 * </p>
 *
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
        ResolutionPlan resolutionPlan = new ResolutionPlan(query);

        ImmutableList<Atom> plan = resolutionPlan.plan();
        TransactionOLTP tx = query.tx();
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

        boolean refine = plan.size() != queries.size() && !query.requiresSchema();
        return refine? refine(queries) : ImmutableList.copyOf(queries);
    }

    private static List<ReasonerQueryImpl> prioritise(QueryCollectionBase queries){
        return queries.stream()
                .sorted(Comparator.comparing(q -> !q.isAtomic()))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isRuleResolvable))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isBoundlesslyDisconnected))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private static ImmutableList<ReasonerQueryImpl> refine(List<ReasonerQueryImpl> qs){
        return ImmutableList.copyOf(refinePlan(new QueryList(qs)).toCollection());
    }

    private static QueryList refinePlan(QueryList queries){
        QueryList plan = new QueryList();
        Stack<ReasonerQueryImpl> queryStack = new Stack<>();

        Lists.reverse(prioritise(queries)).forEach(queryStack::push);
        while(!plan.containsAll(queries)) {
            ReasonerQueryImpl query = queryStack.pop();

            QuerySet candidates = queries.getCandidates(query, plan);

            if (!candidates.isEmpty() || queries.size() - plan.size() == 1){
                plan.add(query);
                Lists.reverse(prioritise(candidates)).forEach(queryStack::push);
            }
        }

        return plan;
    }

}

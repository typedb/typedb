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
import java.util.Collection;
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
        ResolutionPlan resolutionPlan = new ResolutionPlan(query);

        ImmutableList<Atom> plan = resolutionPlan.plan();
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

        boolean refine = plan.size() != queries.size() && !query.requiresSchema();
        return refine? refine(queries) : ImmutableList.copyOf(queries);
    }

    private static Equivalence<ReasonerQuery> equality = ReasonerQueryEquivalence.Equality;

    private static List<Equivalence.Wrapper<ReasonerQueryImpl>> prioritiseQueries(Collection<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        return prioritise(
                queries.stream()
                        .map(Equivalence.Wrapper::get)
                        .collect(Collectors.toList())
        )
                .stream()
                .map(q -> equality.wrap(q))
                .collect(Collectors.toList());
    }

    private static List<ReasonerQueryImpl> prioritise(Collection<ReasonerQueryImpl> queries){
        return queries.stream()
                .sorted(Comparator.comparing(q -> !q.isAtomic()))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isRuleResolvable))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isBoundlesslyDisconnected))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private static ImmutableList<ReasonerQueryImpl> refine(List<ReasonerQueryImpl> qs){
        return ImmutableList.copyOf(
                refinePlan(qs.stream().map(equality::wrap).collect(Collectors.toList()))
                        .stream()
                        .map(Equivalence.Wrapper::get)
                        .collect(Collectors.toList())
        );
    }


    private static boolean isQueryDisconnected(Equivalence.Wrapper<ReasonerQueryImpl> query, Collection<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        return isQueryDisconnected(
                query.get(),
                queries.stream().filter(q -> !q.equals(query)).map(Equivalence.Wrapper::get).collect(Collectors.toList())
        );
    }

    /**
     * @param query of interest
     * @param queries wrt which connectivity is to be determined
     * @return true if query is disconnected wrt queries
     */
    private static boolean isQueryDisconnected(ReasonerQueryImpl query, Collection<ReasonerQueryImpl> queries){
        return queries.stream()
                .allMatch(q -> Sets.intersection(q.getVarNames(), query.getVarNames()).isEmpty());
    }

    private static boolean isQueryReachable(Equivalence.Wrapper<ReasonerQueryImpl> query,
                                            Collection<Equivalence.Wrapper<ReasonerQueryImpl>> target,
                                            Collection<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        return isQueryReachable(
                query.get(),
                target.stream().map(Equivalence.Wrapper::get).collect(Collectors.toList()),
                queries.stream().map(Equivalence.Wrapper::get).collect(Collectors.toList())
        );
    }

    private static boolean isQueryReachable(ReasonerQueryImpl query,
                                            Collection<ReasonerQueryImpl> target,
                                            Collection<ReasonerQueryImpl> queries){
        Set<Var> queryVars = getAllNeighbours(query, queries).stream()
                .flatMap(q -> q.getVarNames().stream())
                .collect(Collectors.toSet());
        return target.stream()
                .anyMatch(tq -> !Sets.intersection(tq.getVarNames(), queryVars).isEmpty());
    }

    private static Set<ReasonerQueryImpl> getAllNeighbours(ReasonerQueryImpl entryQuery, Collection<ReasonerQueryImpl> queries) {
        Set<ReasonerQueryImpl> neighbours = new HashSet<>();
        Set<Equivalence.Wrapper<ReasonerQueryImpl>> visitedQueries = new HashSet<>();
        Stack<Equivalence.Wrapper<ReasonerQueryImpl>> queryStack = new Stack<>();

        Multimap<ReasonerQueryImpl, ReasonerQueryImpl> neighbourMap = createNeighbourMap(queries);
        neighbourMap.get(entryQuery).stream().map(q -> equality.wrap(q)).forEach(queryStack::push);
        while (!queryStack.isEmpty()) {
            Equivalence.Wrapper<ReasonerQueryImpl> wrappedQuery = queryStack.pop();
            ReasonerQueryImpl query = wrappedQuery.get();
            if (!visitedQueries.contains(wrappedQuery) && query != null) {
                neighbourMap.get(query).stream()
                        .peek(neighbours::add)
                        .flatMap(q -> neighbourMap.get(q).stream())
                        .map(equality::wrap)
                        .filter(q -> !visitedQueries.contains(q))
                        .filter(q -> !queryStack.contains(q))
                        .forEach(queryStack::add);
                visitedQueries.add(wrappedQuery);
            }
        }
        return neighbours;
    }

    private static Set<ReasonerQueryImpl> getNeighbours(ReasonerQueryImpl query, Collection<ReasonerQueryImpl> queries){
        Set<Var> vars = query.getVarNames();
        return queries.stream()
                .filter(q2 -> !Sets.intersection(vars, q2.getVarNames()).isEmpty())
                .collect(Collectors.toSet());
    }

    private static Set<Equivalence.Wrapper<ReasonerQueryImpl>> getNeighbours(Equivalence.Wrapper<ReasonerQueryImpl> query, Collection<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        return getNeighbours(
                query.get(),
                queries.stream().filter(q2 -> !query.equals(q2)).map(Equivalence.Wrapper::get).collect(Collectors.toList())
                )
                .stream()
                .map(equality::wrap)
                .collect(Collectors.toSet());
    }

    private static Multimap<ReasonerQueryImpl, ReasonerQueryImpl> createNeighbourMap(Collection<ReasonerQueryImpl> queries){
        Multimap<ReasonerQueryImpl, ReasonerQueryImpl> neighbourMap = HashMultimap.create();
        queries.forEach(q -> neighbourMap.putAll(q, getNeighbours(q, queries)));
        return neighbourMap;
    }

    private static List<Equivalence.Wrapper<ReasonerQueryImpl>> refinePlan(List<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        LinkedList<Equivalence.Wrapper<ReasonerQueryImpl>> plan = new LinkedList<>();
        Stack<Equivalence.Wrapper<ReasonerQueryImpl>> queryStack = new Stack<>();

        Multimap<Equivalence.Wrapper<ReasonerQueryImpl>, Equivalence.Wrapper<ReasonerQueryImpl>> neighbourMap = HashMultimap.create();

        //determine connectivity
        queries.forEach(q -> neighbourMap.putAll(q, getNeighbours(q, queries)));

        Lists.reverse(prioritiseQueries(queries)).forEach(queryStack::push);
        while(!plan.containsAll(queries)) {
            if (queryStack.isEmpty()){
                //backtrack

                Equivalence.Wrapper<ReasonerQueryImpl> last = plan.remove();
                Lists.reverse(prioritiseQueries(queries)).forEach(queryStack::push);
                queryStack.remove(last);
            }
            Equivalence.Wrapper<ReasonerQueryImpl> query = queryStack.pop();

            Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableQueries = queries.stream()
                    .filter(q -> !(plan.contains(q) || q.equals(query)))
                    .collect(Collectors.toSet());

            Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableNeighbours = neighbourMap.get(query).stream()
                    .filter(availableQueries::contains)
                    .collect(Collectors.toSet());

            Set<Var> subbedVars = plan.stream()
                    .map(Equivalence.Wrapper::get)
                    .flatMap(q -> q.getVarNames().stream())
                    .collect(Collectors.toSet());
            Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableNeighboursFromSubs = availableQueries.stream()
                    .map(Equivalence.Wrapper::get)
                    .filter(q -> !Sets.intersection(q.getVarNames(), subbedVars).isEmpty())
                    .map(q -> equality.wrap(q))
                    .collect(Collectors.toSet());

            //candidates
            Set<Equivalence.Wrapper<ReasonerQueryImpl>> candidates = isQueryDisconnected(query, queries)?
                    availableQueries :
                    isQueryReachable(query, availableQueries, queries)? Sets.union(availableNeighbours, availableNeighboursFromSubs): availableQueries;

            if (!candidates.isEmpty() || queries.size() - plan.size() == 1){
                plan.add(query);
                Lists.reverse(prioritiseQueries(candidates)).forEach(queryStack::push);
            }
        }

        return plan;
    }
}

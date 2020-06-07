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

package grakn.core.graql.reasoner.plan;

import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Base class for collections of ReasonerQueryImpl queries with equality comparison ReasonerQueryEquivalence.
 * </p>
 *
 *
 */
public abstract class QueryCollectionBase{

    public abstract Stream<ReasonerQueryImpl> stream();
    public abstract Stream<Equivalence.Wrapper<ReasonerQueryImpl>> wrappedStream();

    ReasonerQueryEquivalence equality(){ return ReasonerQueryEquivalence.Equality;}

    private boolean isQueryDisconnected(Equivalence.Wrapper<ReasonerQueryImpl> query){
        return getImmediateNeighbours(query).isEmpty();
    }

    /**
     * @param query of interest
     * @return true if query is disconnected wrt queries of this collection
     */
    public boolean isQueryDisconnected(ReasonerQueryImpl query){
        return isQueryDisconnected(equality().wrap(query));
    }

    private Set<ReasonerQueryImpl> getImmediateNeighbours(ReasonerQueryImpl query){
        return getImmediateNeighbours(equality().wrap(query))
                .stream()
                .map(Equivalence.Wrapper::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private Set<Equivalence.Wrapper<ReasonerQueryImpl>> getImmediateNeighbours(Equivalence.Wrapper<ReasonerQueryImpl> query){
        ReasonerQueryImpl unwrappedQuery = query.get();
        Set<Variable> vars = unwrappedQuery != null? unwrappedQuery.getVarNames() : new HashSet<>();
        return this.wrappedStream()
                .filter(q2 -> !query.equals(q2))
                .map(Equivalence.Wrapper::get)
                .filter(Objects::nonNull)
                .filter(q2 -> !Sets.intersection(vars, q2.getVarNames()).isEmpty())
                .map(q2 -> equality().wrap(q2))
                .collect(Collectors.toSet());
    }

    private Multimap<ReasonerQueryImpl, ReasonerQueryImpl> immediateNeighbourMap(){
        Multimap<ReasonerQueryImpl, ReasonerQueryImpl> neighbourMap = HashMultimap.create();
        this.stream().forEach(q -> neighbourMap.putAll(q, getImmediateNeighbours(q)));
        return neighbourMap;
    }

    private boolean isQueryReachable(Equivalence.Wrapper<ReasonerQueryImpl> query, Collection<Equivalence.Wrapper<ReasonerQueryImpl>> target){
        return isQueryReachable(query.get(), target.stream().map(Equivalence.Wrapper::get).collect(Collectors.toList()));
    }

    private boolean isQueryReachable(ReasonerQueryImpl query, Collection<ReasonerQueryImpl> target){
        Set<Variable> queryVars = getAllNeighbours(query).stream()
                .flatMap(q -> q.getVarNames().stream())
                .collect(Collectors.toSet());
        return target.stream()
                .anyMatch(tq -> !Sets.intersection(tq.getVarNames(), queryVars).isEmpty());
    }

    private Set<ReasonerQueryImpl> getAllNeighbours(ReasonerQueryImpl entryQuery) {
        Set<ReasonerQueryImpl> neighbours = new HashSet<>();
        Set<Equivalence.Wrapper<ReasonerQueryImpl>> visitedQueries = new HashSet<>();
        Stack<Equivalence.Wrapper<ReasonerQueryImpl>> queryStack = new Stack<>();

        Multimap<ReasonerQueryImpl, ReasonerQueryImpl> neighbourMap = immediateNeighbourMap();

        neighbourMap.get(entryQuery).stream().map(q -> equality().wrap(q)).forEach(queryStack::push);
        while (!queryStack.isEmpty()) {
            Equivalence.Wrapper<ReasonerQueryImpl> wrappedQuery = queryStack.pop();
            ReasonerQueryImpl query = wrappedQuery.get();
            if (!visitedQueries.contains(wrappedQuery) && query != null) {
                neighbourMap.get(query).stream()
                        .peek(neighbours::add)
                        .flatMap(q -> neighbourMap.get(q).stream())
                        .map(q -> equality().wrap(q))
                        .filter(q -> !visitedQueries.contains(q))
                        .filter(q -> !queryStack.contains(q))
                        .forEach(queryStack::add);
                visitedQueries.add(wrappedQuery);
            }
        }
        return neighbours;
    }

    /**
     * @param entryQuery query for which candidates are to be determined
     * @param plan current plan
     * @return set of candidate queries for this query
     */
    QuerySet getCandidates(ReasonerQueryImpl entryQuery, QueryList plan){
        Equivalence.Wrapper<ReasonerQueryImpl> query = equality().wrap(entryQuery);
        Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableQueries = this.wrappedStream()
                .filter(q -> !(plan.contains(q) || q.equals(query)))
                .collect(Collectors.toSet());

        Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableImmediateNeighbours = this.getImmediateNeighbours(query).stream()
                .filter(availableQueries::contains)
                .collect(Collectors.toSet());

        Set<Variable> subbedVars = plan.stream()
                .flatMap(q -> q.getVarNames().stream())
                .collect(Collectors.toSet());

        Set<Equivalence.Wrapper<ReasonerQueryImpl>> availableImmediateNeighboursFromSubs = availableQueries.stream()
                .map(Equivalence.Wrapper::get)
                .filter(Objects::nonNull)
                .filter(q -> !Sets.intersection(q.getVarNames(), subbedVars).isEmpty())
                .map(q -> equality().wrap(q))
                .collect(Collectors.toSet());

        return QuerySet.create(
                this.isQueryDisconnected(query)?
                        availableQueries :
                        this.isQueryReachable(query, availableQueries)?
                                Sets.union(availableImmediateNeighbours, availableImmediateNeighboursFromSubs): availableQueries
        );
    }
}

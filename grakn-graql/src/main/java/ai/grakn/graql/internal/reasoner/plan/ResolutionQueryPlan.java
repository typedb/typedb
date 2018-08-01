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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
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
                top.getAllNeighbours().stream()
                        .filter(atoms::contains)
                        .filter(at -> !at.isRuleResolvable())
                        .peek(atoms::remove)
                        .forEach(nonResolvableAtoms::add);
                if (atoms.isEmpty()) {
                    queries.add(ReasonerQueries.create(nonResolvableAtoms, tx));
                }
            }
        }
        boolean refine = plan.size() != queries.size() && !query.requiresSchema();
        return refine? refine(queries) : ImmutableList.copyOf(queries);
    }

    private static ImmutableList<ReasonerQueryImpl> refine(List<ReasonerQueryImpl> qs){
        return ImmutableList.copyOf(new QueryList(qs).refine().toCollection());
    }

    static List<ReasonerQueryImpl> prioritise(QueryCollectionBase queries){
        return queries.stream()
                .sorted(Comparator.comparing(q -> !q.isAtomic()))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isRuleResolvable))
                .sorted(Comparator.comparing(ReasonerQueryImpl::isBoundlesslyDisconnected))
                .collect(Collectors.toCollection(LinkedList::new));
    }

}

/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * A class for building gremlin traversals from patterns.
 * <p>
 * A {@code Query} is constructed from a single {@code Pattern.Conjunction}. The conjunction is transformed into
 * disjunctive normal form and then an {@code InnerQuery} is constructed from each disjunction component. This allows
 * each {@code InnerQuery} to be described by a single gremlin traversal.
 * <p>
 * The {@code Query} returns a list of gremlin traversals, whose results are combined by {@code MatchQueryImpl} to
 * maintain any requested ordering.
 */
public class Query {

    private final MindmapsTransaction transaction;
    private final Collection<ConjunctionQuery> innerQueries;

    /**
     * @param transaction the transaction to execute the query on
     * @param patternConjunction a pattern to find in the graph
     */
    public Query(MindmapsTransaction transaction, Pattern.Conjunction<?> patternConjunction) {
        Collection<Pattern.Conjunction<Var.Admin>> patterns =
                patternConjunction.getDisjunctiveNormalForm().getPatterns();

        if (transaction == null) {
            throw new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage());
        }

        this.transaction = transaction;

        innerQueries = patterns.stream().map(pattern -> new ConjunctionQuery(transaction, pattern)).collect(toList());
    }

    /**
     * @return a gremlin traversal to execute to find results
     */
    public GraphTraversal<Vertex, Map<String, Vertex>> getTraversals() {
        GraphTraversal[] collect =
                innerQueries.stream().map(ConjunctionQuery::getTraversal).toArray(GraphTraversal[]::new);

        // Because 'union' accepts an array, we can't use generics...
        //noinspection unchecked
        return ((MindmapsTransactionImpl) transaction).getTinkerPopGraph().traversal().V().limit(1).union(collect);
    }

    /**
     * @return a stream of concept IDs mentioned in the query
     */
    public Stream<String> getConcepts() {
        return innerQueries.stream().flatMap(ConjunctionQuery::getConcepts);
    }

}

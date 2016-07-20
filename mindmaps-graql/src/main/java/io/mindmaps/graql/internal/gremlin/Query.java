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

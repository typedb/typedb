package io.mindmaps.graql.internal.query;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.AskQuery;
import io.mindmaps.graql.api.query.MatchQuery;

/**
 * An AskQuery to check if a given pattern matches anywhere in the graph
 */
public class AskQueryImpl implements AskQuery.Admin {

    private final MatchQuery matchQuery;

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    public AskQueryImpl(MatchQuery matchQuery) {
        this.matchQuery = matchQuery;
    }

    @Override
    public boolean execute() {
        return matchQuery.iterator().hasNext();
    }

    @Override
    public AskQuery withTransaction(MindmapsTransaction transaction) {
        matchQuery.withTransaction(transaction);
        return this;
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " ask";
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }
}
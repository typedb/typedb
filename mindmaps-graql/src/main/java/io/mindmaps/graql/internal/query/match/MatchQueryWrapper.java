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
 *
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.MatchQueryDefaultAdmin;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.query.Conjunction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Wrapper class that wraps up a {@link AbstractMatchQuery} into a {@link AbstractMatchQueryDefault}, provided it operates on
 * {@code Map<String, Concept>}. This is used to wrap up classes such as {@link MatchQueryLimit}.
 */
public class MatchQueryWrapper extends AbstractMatchQueryDefault {

    private final MatchQueryAdmin<Map<String, Concept>> query;
    private final MatchQueryDefaultAdmin queryMap;

    /**
     * @param query the query to wrap
     * @param queryMap the original {@link AbstractMatchQueryDefault} that {@code query} was derived from
     */
    public MatchQueryWrapper(MatchQueryAdmin<Map<String, Concept>> query, MatchQueryDefaultAdmin queryMap) {
        this.query = query;
        this.queryMap = queryMap;
    }

    @Override
    public Stream<Map<String, Concept>> stream(Optional<MindmapsTransaction> transaction, Optional<MatchOrder> order) {
        return query.stream(transaction, order);
    }

    @Override
    public Set<Type> getTypes(MindmapsTransaction transaction) {
        return query.getTypes(transaction);
    }

    @Override
    public Set<Type> getTypes() {
        return query.getTypes();
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return queryMap.getPattern();
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return query.getTransaction();
    }

    @Override
    public Set<String> getSelectedNames() {
        return queryMap.getSelectedNames();
    }

    @Override
    public String toString() {
        return query.toString();
    }
}

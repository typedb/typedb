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

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.MatchQueryMap;
import io.mindmaps.graql.Pattern;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class MatchQueryWrapper implements MatchQueryMap.Admin {

    private final MatchQuery.Admin<Map<String, Concept>> query;
    private final MatchQueryMap.Admin queryMap;

    public MatchQueryWrapper(MatchQuery.Admin<Map<String, Concept>> query, MatchQueryMap.Admin queryMap) {
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
    public Pattern.Conjunction<Pattern.Admin> getPattern() {
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

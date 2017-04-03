/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Answer;

import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.util.ErrorMessage.NON_POSITIVE_LIMIT;

/**
 * "Limit" modifier for match query that limits the results of a query.
 */
class MatchQueryLimit extends MatchQueryModifier {

    private final long limit;

    MatchQueryLimit(AbstractMatchQuery inner, long limit) {
        super(inner);
        if (limit <= 0) {
            throw new IllegalArgumentException(NON_POSITIVE_LIMIT.getMessage(limit));
        }
        this.limit = limit;
    }

    @Override
    public Stream<Answer> stream(Optional<GraknGraph> graph) {
        return inner.stream(graph).limit(limit);
    }

    @Override
    protected String modifierString() {
        return " limit " + limit + ";";
    }
}

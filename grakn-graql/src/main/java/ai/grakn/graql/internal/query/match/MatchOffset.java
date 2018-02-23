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

package ai.grakn.graql.internal.query.match;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.admin.Answer;
import ai.grakn.kb.internal.EmbeddedGraknTx;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * "Offset" modifier for {@link Match} that offsets (skips) some number of results.
 */
class MatchOffset extends MatchModifier {

    private final long offset;

    MatchOffset(AbstractMatch inner, long offset) {
        super(inner);
        if (offset < 0) {
            throw GraqlQueryException.negativeOffset(offset);
        }
        this.offset = offset;
    }

    @Override
    public Stream<Answer> stream(Optional<EmbeddedGraknTx<?>> graph) {
        return inner.stream(graph).skip(offset);
    }

    @Override
    protected String modifierString() {
        return " offset " + offset + ";";
    }
}

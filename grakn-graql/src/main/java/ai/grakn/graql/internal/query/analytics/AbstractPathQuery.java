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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.nullableIdToString;
import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static java.util.stream.Collectors.joining;


abstract class AbstractPathQuery<T, V extends ComputeQuery<T>>
        extends AbstractComputeQuery<T, V> {

    private @Nullable ConceptId from = null;
    private @Nullable ConceptId to = null;

    AbstractPathQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    public final V from(ConceptId sourceId) {
        this.from = sourceId;
        return (V) this;
    }

    public final ConceptId from() {
        if (from == null) throw GraqlQueryException.noPathSource();
        return from;
    }

    public V to(ConceptId destinationId) {
        this.to = destinationId;
        return (V) this;
    }

    public final ConceptId to() {
        if (to == null) throw GraqlQueryException.noPathDestination();
        return to;
    }

    abstract String methodString();

    final String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        conditionsList.add(FROM + SPACE + nullableIdToString(from));
        conditionsList.add(TO + SPACE + nullableIdToString(to));
        if (!inTypesString().isEmpty()) conditionsList.add(inTypesString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }
}

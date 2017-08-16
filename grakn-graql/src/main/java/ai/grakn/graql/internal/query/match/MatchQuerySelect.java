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

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * "Select" modifier for a match query that selects particular variables from the result.
 */
class MatchQuerySelect extends MatchQueryModifier {

    private final ImmutableSet<Var> names;

    MatchQuerySelect(AbstractMatchQuery inner, ImmutableSet<Var> names) {
        super(inner);

        Set<Var> selectedNames = inner.getSelectedNames();

        for (Var name : names) {
            if (!selectedNames.contains(name)) {
                throw GraqlQueryException.varNotInQuery(name);
            }
        }

        if (names.isEmpty()) {
            throw GraqlQueryException.noSelectedVars();
        }

        this.names = names;
    }

    @Override
    public Stream<Answer> stream(Optional<GraknTx> graph) {
        return inner.stream(graph).map(result -> result.filterVars(names));
    }

    @Override
    protected String modifierString() {
        return " select " + names.stream().map(Object::toString).collect(joining(", ")) + ";";
    }

    @Override
    public Set<Var> getSelectedNames() {
        return names;
    }
}

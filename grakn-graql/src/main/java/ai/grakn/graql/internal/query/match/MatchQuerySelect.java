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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * "Select" modifier for a match query that selects particular variables from the result.
 */
class MatchQuerySelect extends MatchQueryModifier {

    private final ImmutableSet<VarName> names;

    MatchQuerySelect(AbstractMatchQuery inner, ImmutableSet<VarName> names) {
        super(inner);

        Set<VarName> selectedNames = inner.getSelectedNames();

        for (VarName name : names) {
            if (!selectedNames.contains(name)) {
                throw new IllegalArgumentException(ErrorMessage.VARIABLE_NOT_IN_QUERY.getMessage(name));
            }
        }

        if (names.isEmpty()) {
            throw new IllegalArgumentException(ErrorMessage.SELECT_NONE_SELECTED.getMessage());
        }

        this.names = names;
    }

    @Override
    public Stream<Answer> stream(Optional<GraknGraph> graph) {
        return inner.stream(graph).map(result -> result.filterVars(names));
    }

    @Override
    protected String modifierString() {
        return " select " + names.stream().map(Object::toString).collect(joining(", ")) + ";";
    }

    @Override
    public Set<VarName> getSelectedNames() {
        return names;
    }
}

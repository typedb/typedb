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

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.analytics.ConnectedComponentQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Arg.MEMBERS;
import static ai.grakn.util.GraqlSyntax.Compute.Arg.SIZE;
import static ai.grakn.util.GraqlSyntax.Compute.Arg.START;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.WHERE;
import static ai.grakn.util.GraqlSyntax.EQUAL;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static ai.grakn.util.GraqlSyntax.SQUARE_CLOSE;
import static ai.grakn.util.GraqlSyntax.SQUARE_OPEN;
import static java.util.stream.Collectors.joining;

class ConnectedComponentQueryImpl<T> extends AbstractClusterQuery<T, ConnectedComponentQuery<T>>
        implements ConnectedComponentQuery<T> {

    private boolean members = false;
    private Optional<ConceptId> start = Optional.empty();
    private Optional<Long> size = Optional.empty();

    ConnectedComponentQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<T> executeComputer() {
        return queryComputer().run(this);
    }

    @Override
    public ConnectedComponentQuery<Map<String, Set<String>>> membersOn() {
        this.members = true;
        return (ConnectedComponentQuery<Map<String, Set<String>>>) this;
    }

    @Override
    public ConnectedComponentQuery<Map<String, Long>> membersOff() {
        this.members = false;
        return (ConnectedComponentQuery<Map<String, Long>>) this;
    }

    @Override
    public final boolean isMembersSet() {
        return members;
    }

    @Override
    public ConnectedComponentQuery<T> start(ConceptId conceptId) {
        this.start = Optional.ofNullable(conceptId);
        return this;
    }

    @Override
    public final Optional<ConceptId> start() {
        return start;
    }

    @Override
    public ConnectedComponentQuery<T> size(long size) {
        this.size = Optional.of(size);
        return this;
    }

    @Override
    public final Optional<Long> size() {
        return size;
    }

    @Override
    String algorithmString() {
        return USING + SPACE + CONNECTED_COMPONENT;
    }

    @Override
    String argumentsString() {
        List<String> argumentsList = new ArrayList<>();
        StringBuilder argumentsString = new StringBuilder();

        if (start().isPresent()) argumentsList.add(START + EQUAL + start().get());
        if (size().isPresent()) argumentsList.add(SIZE + EQUAL + size().get());
        if (isMembersSet()) argumentsList.add(MEMBERS + EQUAL + members);

        if(!argumentsList.isEmpty()) {
            argumentsString.append(WHERE + SPACE);
            if(argumentsList.size() == 1) argumentsString.append(argumentsList.get(0));
            else {
                argumentsString.append(SQUARE_OPEN);
                argumentsString.append(argumentsList.stream().collect(joining(COMMA_SPACE)));
                argumentsString.append(SQUARE_CLOSE);
            }
        }

        return argumentsString.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ConnectedComponentQueryImpl<?> that = (ConnectedComponentQueryImpl<?>) o;

        return start.equals(that.start) && members == that.members && size().equals(that.size());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + CONNECTED_COMPONENT.hashCode();
        result = 31 * result + start.hashCode();
        result = 31 * result + Boolean.hashCode(members);
        result = 31 * result + size.hashCode();
        return result;
    }
}

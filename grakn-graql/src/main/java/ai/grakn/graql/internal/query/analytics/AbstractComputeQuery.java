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
import ai.grakn.concept.Label;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.internal.query.AbstractExecutableQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.COMPUTE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.IN;
import static ai.grakn.util.GraqlSyntax.SEMICOLON;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static ai.grakn.util.GraqlSyntax.SQUARE_CLOSE;
import static ai.grakn.util.GraqlSyntax.SQUARE_OPEN;
import static java.util.stream.Collectors.joining;

abstract class AbstractComputeQuery<T, V extends ComputeQuery<T>>
        extends AbstractExecutableQuery<T> implements ComputeQuery<T> {

    private Optional<GraknTx> tx;
    private boolean includeAttribute;
    protected ImmutableSet<Label> inTypes = ImmutableSet.of();

    private Set<ComputeJob<T>> runningJobs = ConcurrentHashMap.newKeySet();

    private static final boolean DEFAULT_INCLUDE_ATTRIBUTE = false;

    AbstractComputeQuery(Optional<GraknTx> tx) {
        this(tx, DEFAULT_INCLUDE_ATTRIBUTE);
    }

    AbstractComputeQuery(Optional<GraknTx> tx, boolean includeAttribute) {
        this.tx = tx;
        this.includeAttribute = includeAttribute;
    }

    @Override
    public final T execute() {
        ComputeJob<T> job = executeComputer();

        runningJobs.add(job);

        try {
            return job.get();
        } finally {
            runningJobs.remove(job);
        }
    }

    protected abstract ComputeJob<T> executeComputer();

    @Override
    public final Optional<GraknTx> tx() {
        return tx;
    }

    @Override
    public final V withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return (V) this;
    }

    @Override
    public final V in(String... inTypes) {
        return in(Arrays.stream(inTypes).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final V in(Collection<? extends Label> inTypes) {
        this.inTypes = ImmutableSet.copyOf(inTypes);
        return (V) this;
    }

    @Override
    public final ImmutableSet<Label> inTypes() {
        return inTypes;
    }

    @Override
    public final V includeAttribute() {
        this.includeAttribute = true;
        return (V) this;
    }

    @Override
    public final boolean isAttributeIncluded() {
        return includeAttribute;
    }

    @Override
    public final void kill() {
        runningJobs.forEach(ComputeJob::kill);
    }

    abstract String methodString();

    abstract String conditionsString();

    final String inTypesString() {
        if (!inTypes.isEmpty()) return IN + SPACE + typesString(inTypes);

        return "";
    }

    final String typesString(ImmutableSet<Label> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) inTypesString.append(StringConverter.typeLabelToString(types.iterator().next()));
            else {
                inTypesString.append(SQUARE_OPEN);
                inTypesString.append(inTypes.stream().map(StringConverter::typeLabelToString).collect(joining(COMMA_SPACE)));
                inTypesString.append(SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(COMPUTE + SPACE + methodString());

        if (!conditionsString().isEmpty()) {
            query.append(SPACE + conditionsString());
        }

        query.append(SEMICOLON);

        return query.toString();
    }

    @Nullable
    @Override
    public Boolean inferring() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractComputeQuery<?, ?> that = (AbstractComputeQuery<?, ?>) o;

        return tx.equals(that.tx) && includeAttribute == that.includeAttribute && inTypes.equals(that.inTypes);
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        result = 31 * result + inTypes.hashCode();
        return result;
    }
}

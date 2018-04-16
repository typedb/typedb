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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.VarPattern;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class UndefineQueryImpl extends AbstractQuery<Void, Void> implements UndefineQuery {

    static UndefineQueryImpl of(Collection<? extends VarPattern> varPatterns, @Nullable GraknTx tx) {
        return new AutoValue_UndefineQueryImpl(Optional.ofNullable(tx), ImmutableList.copyOf(varPatterns));
    }

    @Override
    public UndefineQuery withTx(GraknTx tx) {
        return of(varPatterns(), tx);
    }

    @Override
    public final Void execute() {
        queryRunner().run(this);
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String toString() {
        return "undefine " + varPatterns().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    protected final Stream<Void> stream() {
        execute();
        return Stream.empty();
    }

    @Nullable
    @Override
    public Boolean inferring() {
        return null;
    }
}

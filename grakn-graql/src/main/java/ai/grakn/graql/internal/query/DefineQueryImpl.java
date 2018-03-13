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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation for {@link DefineQuery}
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class DefineQueryImpl extends AbstractExecutableQuery<Answer> implements DefineQuery {

    static DefineQueryImpl of(Collection<? extends VarPattern> varPatterns, @Nullable GraknTx tx) {
        return new AutoValue_DefineQueryImpl(Optional.ofNullable(tx), ImmutableList.copyOf(varPatterns));
    }

    @Override
    public Query<Answer> withTx(GraknTx tx) {
        return DefineQueryImpl.of(varPatterns(), tx);
    }

    @Override
    public final Answer execute() {
        return queryRunner().run(this);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String toString() {
        return "define " + varPatterns().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Nullable
    @Override
    public Boolean inferring() {
        return null;
    }
}

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
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableList;

/**
 * Implementation for {@link DefineQuery}
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class DefineQueryImpl implements DefineQuery {

    abstract ImmutableList<VarPatternAdmin> varPatterns();
    abstract @Nullable GraknTx tx();

    static DefineQueryImpl of(ImmutableList<VarPatternAdmin> varPatterns, @Nullable GraknTx tx) {
        return new AutoValue_DefineQueryImpl(varPatterns, tx);
    }

    @Override
    public Query<Answer> withTx(GraknTx tx) {
        return DefineQueryImpl.of(varPatterns(), tx);
    }

    @Override
    public Answer execute() {
        GraknTx tx = tx();
        if (tx == null) throw GraqlQueryException.noTx();

        ImmutableList<VarPatternAdmin> allPatterns =
                varPatterns().stream().flatMap(v -> v.innerVarPatterns().stream()).collect(toImmutableList());
        
        return QueryOperationExecutor.defineAll(allPatterns, tx);
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        return Stream.of(printer.graqlString(execute()));
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String toString() {
        return "define " + varPatterns().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }
}

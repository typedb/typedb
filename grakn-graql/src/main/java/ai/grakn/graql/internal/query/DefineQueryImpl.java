/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Implementation for {@link DefineQuery}
 */
@AutoValue
abstract class DefineQueryImpl extends AbstractExecutableQuery<ConceptMap> implements DefineQuery {

    static DefineQueryImpl of(Collection<? extends VarPattern> varPatterns, @Nullable GraknTx tx) {
        return new AutoValue_DefineQueryImpl(tx, ImmutableList.copyOf(varPatterns));
    }

    @Override
    public Query<ConceptMap> withTx(GraknTx tx) {
        return DefineQueryImpl.of(varPatterns(), tx);
    }

    @Override
    public final ConceptMap execute() {
        return executor().run(this);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String toString() {
        return "define " + varPatterns().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public Boolean inferring() {
        return false;
    }
}

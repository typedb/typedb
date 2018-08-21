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
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation for {@link UndefineQuery}
 *
 * @author Grakn Warriors
 */
@AutoValue
abstract class UndefineQueryImpl implements UndefineQuery {

    static UndefineQueryImpl of(Collection<? extends VarPattern> varPatterns, @Nullable GraknTx tx) {
        return new AutoValue_UndefineQueryImpl(tx, ImmutableList.copyOf(varPatterns));
    }

    @Override
    public UndefineQuery withTx(GraknTx tx) {
        return of(varPatterns(), tx);
    }

    @Override
    public Stream<ConceptMap> stream() {
        return executor().run(this);
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
    public Boolean inferring() {
        return false;
    }
}

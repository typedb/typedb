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

package grakn.core.graql.query;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.server.Transaction;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A query for undefining the Schema types.
 * The query will undefine all concepts described in the pattern provided.
 */
@AutoValue
public abstract class UndefineQuery implements Query<ConceptMap> {

    static UndefineQuery of(Collection<? extends Statement> varPatterns, @Nullable Transaction tx) {
        return new AutoValue_UndefineQuery(tx, ImmutableList.copyOf(varPatterns));
    }

    @Override
    public UndefineQuery withTx(Transaction tx) {
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

    /**
     * Get the {@link Statement}s describing what {@link SchemaConcept}s to define.
     */
    public abstract Collection<? extends Statement> varPatterns();
}

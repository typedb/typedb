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

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A query for undefining the Schema types.
 * The query will undefine all concepts described in the pattern provided.
 */
public class UndefineQuery implements Query<ConceptMap> {

    private final Transaction tx;
    private final Collection<? extends Statement> statements;

    UndefineQuery(@Nullable Transaction tx, Collection<? extends Statement> statements) {
        this.tx = tx;
        if (statements == null) {
            throw new NullPointerException("Null statements");
        }
        this.statements = statements;
    }

    @Nullable
    @Override
    public Transaction tx() {
        return tx;
    }

    /**
     * Get the {@link Statement}s describing what {@link SchemaConcept}s to define.
     */
    public Collection<? extends Statement> statements() {
        return statements;
    }

    @Override
    public Stream<ConceptMap> stream() {
        return executor().run(this);
    }

    @Override
    public String toString() {
        return "undefine " + statements().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof UndefineQuery) {
            UndefineQuery that = (UndefineQuery) o;
            return ((this.tx == null) ? (that.tx() == null) : this.tx.equals(that.tx()))
                    && (this.statements.equals(that.statements()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (tx == null) ? 0 : this.tx.hashCode();
        h *= 1000003;
        h ^= this.statements.hashCode();
        return h;
    }
}

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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.admin.Answer;
import ai.grakn.kb.internal.EmbeddedGraknTx;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 */
class MatchTx extends MatchModifier {

    private final GraknTx tx;

    MatchTx(GraknTx tx, AbstractMatch inner) {
        super(inner);
        this.tx = tx;
    }

    @Override
    public Stream<Answer> stream(Optional<EmbeddedGraknTx<?>> graph) {
        if (graph.isPresent()) {
            throw GraqlQueryException.multipleTxs();
        }

        // TODO: This cast is unsafe - this is fixed if queries don't contain transactions
        return inner.stream(Optional.of((EmbeddedGraknTx<?>) this.tx));
    }

    @Override
    public Optional<GraknTx> tx() {
        return Optional.of(tx);
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        return inner.getSchemaConcepts(tx);
    }

    @Override
    protected String modifierString() {
        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MatchTx maps = (MatchTx) o;

        return tx.equals(maps.tx);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + tx.hashCode();
        return result;
    }
}

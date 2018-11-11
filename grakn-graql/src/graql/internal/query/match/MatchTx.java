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

package grakn.core.graql.internal.query.match;

import grakn.core.GraknTx;
import grakn.core.concept.SchemaConcept;
import grakn.core.exception.GraqlQueryException;
import grakn.core.graql.Match;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.kb.internal.EmbeddedGraknTx;

import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 *
 * @author Grakn Warriors
 */
class MatchTx extends MatchModifier {

    private final GraknTx tx;

    MatchTx(GraknTx tx, AbstractMatch inner) {
        super(inner);
        this.tx = tx;
    }

    @Override
    public Stream<ConceptMap> stream(EmbeddedGraknTx<?> tx) {
        // TODO: This is dodgy. We need to refactor the code to remove this behavior
        if (tx != null) throw GraqlQueryException.multipleTxs();

        // TODO: This cast is unsafe - this is fixed if queries don't contain transactions
        return inner.stream((EmbeddedGraknTx<?>) this.tx);
    }

    @Override
    public GraknTx tx() {
        return tx;
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

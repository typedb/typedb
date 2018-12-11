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

import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;

import java.util.Set;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 */
class MatchInfer extends Match {

    final Match inner;

    MatchInfer(Match inner) {
        super();
        this.inner = inner;
    }

    @Override
    public final Boolean inferring() {
        return true;
    }

    @Override
    public final Conjunction<Pattern> getPatterns() {
        return inner.getPatterns();
    }

    @Override
    public Transaction tx() {
        return inner.tx();
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        return inner.getSchemaConcepts();
    }

    @Override
    public final Set<Variable> getSelectedNames() {
        return inner.getSelectedNames();
    }

    @Override
    public final String toString() {
        return inner.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchInfer maps = (MatchInfer) o;

        return inner.equals(maps.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}

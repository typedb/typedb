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
import grakn.core.graql.Match;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.PatternAdmin;

import java.util.Set;

/**
 * A {@link Match} implementation, which contains an 'inner' {@link Match}.
 *
 * This class behaves like a singly-linked list, referencing another {@link Match} until it reaches a {@link MatchBase}.
 *
 * Query modifiers should extend this class and implement a stream() method that modifies the inner query.
 *
 */
abstract class MatchModifier extends AbstractMatch {

    final AbstractMatch inner;

    MatchModifier(AbstractMatch inner) {
        this.inner = inner;
    }

    @Override
    public final Set<SchemaConcept> getSchemaConcepts(GraknTx tx) {
        return inner.getSchemaConcepts(tx);
    }

    @Override
    public final Conjunction<PatternAdmin> getPattern() {
        return inner.getPattern();
    }

    @Override
    public GraknTx tx() {
        return inner.tx();
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        return inner.getSchemaConcepts();
    }

    @Override
    public final Set<Var> getSelectedNames() {
        return inner.getSelectedNames();
    }

    /**
     * @return a string representation of this modifier
     */
    protected abstract String modifierString();

    @Override
    public Boolean inferring() {
        return inner.inferring();
    }

    @Override
    public final String toString() {
        return inner.toString() + modifierString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchModifier maps = (MatchModifier) o;

        return inner.equals(maps.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}

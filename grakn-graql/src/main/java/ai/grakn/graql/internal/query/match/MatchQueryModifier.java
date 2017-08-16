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
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;

import java.util.Optional;
import java.util.Set;

/**
 * A MatchQuery implementation, which contains an 'inner' MatchQuery.
 *
 * This class behaves like a singly-linked list, referencing another MatchQuery until it reaches a MatchQueryBase.
 *
 * Query modifiers should extend this class and implement a stream() method that modifies the inner query.
 */
abstract class MatchQueryModifier extends AbstractMatchQuery {

    final AbstractMatchQuery inner;

    MatchQueryModifier(AbstractMatchQuery inner) {
        this.inner = inner;
    }

    @Override
    public final Set<OntologyConcept> getOntologyConcepts(GraknTx graph) {
        return inner.getOntologyConcepts(graph);
    }

    @Override
    public final Conjunction<PatternAdmin> getPattern() {
        return inner.getPattern();
    }

    @Override
    public Optional<GraknTx> getGraph() {
        return inner.getGraph();
    }

    @Override
    public Set<OntologyConcept> getOntologyConcepts() {
        return inner.getOntologyConcepts();
    }

    @Override
    public Set<Var> getSelectedNames() {
        return inner.getSelectedNames();
    }

    /**
     * @return a string representation of this modifier
     */
    protected abstract String modifierString();

    @Override
    public final String toString() {
        return inner.toString() + modifierString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchQueryModifier maps = (MatchQueryModifier) o;

        return inner.equals(maps.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}

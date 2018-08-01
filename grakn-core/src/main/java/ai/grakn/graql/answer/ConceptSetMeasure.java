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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.answer;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Explanation;

import java.util.Set;

/**
 * A type of {@link Answer} object that contains a {@link Set} and {@link Number}, by extending {@link ConceptSet}.
 */
public class ConceptSetMeasure extends ConceptSet{

    private final Number measurement;

    public ConceptSetMeasure(Set<ConceptId> set, Number measurement) {
        this(set, measurement, null);
    }

    public ConceptSetMeasure(Set<ConceptId> set, Number measurement, Explanation explanation) {
        super(set, explanation);
        this.measurement = measurement;
    }

    @Override
    public ConceptSetMeasure get() {
        return this;
    }

    public Number measurement() {
        return measurement;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ConceptSetMeasure a2 = (ConceptSetMeasure) obj;
        return this.set().equals(a2.set())
                && measurement.toString().equals(a2.measurement.toString());
    }

    @Override
    public int hashCode(){
        int hash = super.hashCode();
        hash = 31 * hash + measurement.hashCode();

        return hash;
    }
}

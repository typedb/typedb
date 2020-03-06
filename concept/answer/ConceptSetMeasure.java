/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.answer;

import grakn.core.kb.concept.api.ConceptId;

import java.util.Set;

/**
 * A type of Answer object that contains a Set and Number, by extending ConceptSet.
 */
public class ConceptSetMeasure extends ConceptSet {

    private final Number measurement;

    public ConceptSetMeasure(Set<ConceptId> set, Number measurement) {
        super(set);
        this.measurement = measurement;
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
    public int hashCode() {
        int hash = super.hashCode();
        hash = 31 * hash + measurement.hashCode();

        return hash;
    }
}

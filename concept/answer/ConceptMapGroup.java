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

import grakn.core.concept.Concept;

import java.util.List;

public class ConceptMapGroup implements Answer {
    private final Concept owner;
    private final List<ConceptMap> conceptMaps;

    public ConceptMapGroup(Concept owner, List<ConceptMap> conceptMaps) {
        this.owner = owner;
        this.conceptMaps = conceptMaps;
    }

    public Concept owner() {
        return this.owner;
    }

    public List<ConceptMap> conceptMaps() {
        return this.conceptMaps;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final ConceptMapGroup a2 = (ConceptMapGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.conceptMaps.equals(a2.conceptMaps);
    }

    @Override
    public int hashCode() {
        int hash = owner.hashCode();
        hash = 31 * hash + conceptMaps.hashCode();

        return hash;
    }
}

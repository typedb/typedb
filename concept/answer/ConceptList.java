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

import java.util.Collections;
import java.util.List;

/**
 * A type of Answer object that contains a List of Concepts.
 */
public class ConceptList extends Answer{

    // TODO: change to store List<Concept> once we are able to construct Concept without a database look up
    private final List<ConceptId> list;

    public ConceptList(List<ConceptId> list) {
        this.list = Collections.unmodifiableList(list);
    }


    public List<ConceptId> list() {
        return list;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ConceptList a2 = (ConceptList) obj;
        return this.list.equals(a2.list);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }
}

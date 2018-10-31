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

package ai.grakn.graql.answer;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Explanation;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A type of {@link Answer} object that contains a {@link Set}.
 */
public class ConceptSet implements Answer<ConceptSet>{

    // TODO: change to store Set<Concept> once we are able to construct Concept without a database look up
    private final Set<ConceptId> set;
    private final Explanation explanation;

    public ConceptSet(Set<ConceptId> set) {
        this(set, null);
    }

    public ConceptSet(Set<ConceptId> set, Explanation explanation) {
        this.set = ImmutableSet.copyOf(set);
        this.explanation = explanation;
    }
    
    @Override
    public ConceptSet asConceptSet() {
        return this;
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    public Set<ConceptId> set() {
        return set;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ConceptSet a2 = (ConceptSet) obj;
        return this.set.equals(a2.set);
    }

    @Override
    public int hashCode(){
        return set.hashCode();
    }
}

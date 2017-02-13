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

package ai.grakn.concept;

import java.io.Serializable;

/**
 * <p>
 *     A Concept Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link Concept} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing concept IDs from Strings.
 * </p>
 *
 * @author fppt
 */
public class ConceptId implements Comparable<ConceptId>, Serializable {
    private Object conceptId;

    private ConceptId(Object conceptId){
        this.conceptId = conceptId;
    }

    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    public String getValue(){
        return conceptId.toString();
    }

    /**
     *
     * @return the raw vertex id. This is used for traversing across vertices directly.
     */
    public Object getRawValue(){
        return conceptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConceptId conceptId1 = (ConceptId) o;

        return conceptId.equals(conceptId1.conceptId);
    }

    @Override
    public int hashCode() {
        return conceptId.hashCode();
    }

    @Override
    public int compareTo(ConceptId o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public String toString(){
        return getValue();
    }

    /**
     *
     * @param value The string which potentially represents a Concept
     * @return The matching concept ID
     */
    public static ConceptId of(Object value){
        return new ConceptId(value);
    }
}

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

import com.google.common.base.Preconditions;

import javax.annotation.CheckReturnValue;
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
    private static final long serialVersionUID = -1723590529071614152L;

    private String conceptId;
    private int hashCode = 0;

    private ConceptId(String conceptId){
        this.conceptId = conceptId;
    }

    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    public String getValue(){
        return conceptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConceptId cast = (ConceptId) o;
        return conceptId.equals(cast.conceptId);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0 ){
            hashCode = conceptId.hashCode();
        }
        return hashCode;
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
    @CheckReturnValue
    public static ConceptId of(String value){
        Preconditions.checkNotNull(value);
        return new ConceptId(value);
    }
}

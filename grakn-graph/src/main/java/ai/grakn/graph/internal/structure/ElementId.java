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

package ai.grakn.graph.internal.structure;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * <p>
 *     A Concept Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link AbstractElement} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing {@link AbstractElement} IDs from Strings.
 * </p>
 *
 * @author fppt
 */
public class ElementId implements Serializable {

    private static final long serialVersionUID = 6688475951939464790L;
    private final String elementId;
    private int hashCode = 0;

    private ElementId(String conceptId){
        this.elementId = conceptId;
    }

    @CheckReturnValue
    public String getValue(){
        return elementId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElementId cast = (ElementId) o;
        return elementId.equals(cast.elementId);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0 ){
            hashCode = elementId.hashCode();
        }
        return hashCode;
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
    public static ElementId of(String value){
        return new ElementId(value);
    }
}

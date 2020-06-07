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

package grakn.core.kb.concept.api;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * A class which represents an id of any Concept.
 * Also contains a static method for producing concept IDs from Strings.
 */
public class ConceptId implements Comparable<ConceptId>, Serializable {

    private static final long serialVersionUID = -1723590529071614152L;
    private final String value;

    /**
     * A non-argument constructor for ConceptID, for serialisation of OLAP queries dependencies
     */
    ConceptId() {
        this.value = null;
    }

    /**
     * The default constructor for ConceptID, which requires String value provided
     *
     * @param value String representation of the Concept ID
     */
    ConceptId(String value) {
        if (value == null) throw new NullPointerException("Provided ConceptId is NULL");

        this.value = value;
    }

    /**
     * @param value The string which potentially represents a Concept
     * @return The matching concept ID
     */
    @CheckReturnValue
    public static ConceptId of(String value) {
        return new ConceptId(value);
    }

    /**
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(ConceptId o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public final String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        ConceptId that = (ConceptId) o;
        return (this.value.equals(that.getValue()));
    }

    @Override
    public int hashCode() {
        int result = 31 * this.value.hashCode();
        return result;
    }
}

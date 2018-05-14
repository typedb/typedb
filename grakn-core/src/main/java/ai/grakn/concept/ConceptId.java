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

package ai.grakn.concept;

import ai.grakn.GraknTx;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * A class which represents an id of any {@link Concept} in the {@link GraknTx}.
 * Also contains a static method for producing concept IDs from Strings.
 *
 * @author Haikal Pribadi
 */
public class ConceptId implements Comparable<ConceptId>, Serializable {

    private final String value;

    /**
     * A non-argument constructor for ConceptID, for serialisation of OLAP queries dependencies
     */
    ConceptId() {
        this.value = null;
    }

    /**
     * The default constructor for ConceptID, which requires String value provided
     * @param value String representation of the Concept ID
     */
    ConceptId(String value) {
        if(value == null) throw new NullPointerException("Provided ConceptId is NULL");

        this.value = value;
    }
    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(ConceptId o) {
        return getValue().compareTo(o.getValue());
    }

    /**
     *
     * @param value The string which potentially represents a Concept
     * @return The matching concept ID
     */
    @CheckReturnValue
    @JsonCreator
    public static ConceptId of(String value){
        return new ConceptId(value);
    }

    @Override
    public final String toString() {
        // TODO: Consider using @AutoValue toString
        return getValue();
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

    private static final long serialVersionUID = -1723590529071614152L;
}

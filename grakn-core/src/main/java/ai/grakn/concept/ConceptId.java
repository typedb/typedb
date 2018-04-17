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
import com.google.auto.value.AutoValue;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * <p>
 *     A Concept Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link Concept} in the {@link GraknTx}.
 *     Also contains a static method for producing concept IDs from Strings.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class ConceptId implements Comparable<ConceptId>, Serializable {
    private static final long serialVersionUID = -1723590529071614152L;

    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    @JsonValue
    public abstract String getValue();

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
        return new AutoValue_ConceptId(value);
    }

    @Override
    public final String toString() {
        // TODO: Consider using @AutoValue toString
        return getValue();
    }
}

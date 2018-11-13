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

package grakn.core.concept;

import grakn.core.server.Transaction;
import com.google.auto.value.AutoValue;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * <p>
 *     A Type Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link SchemaConcept} in the {@link Transaction}.
 *     Also contains a static method for producing IDs from Integers.
 * </p>
 *
 */
@AutoValue
public abstract class LabelId implements Comparable<LabelId>, Serializable {
    private static final long serialVersionUID = -1676610785035926909L;

    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    public abstract Integer getValue();

    @Override
    public int compareTo(LabelId o) {
        return getValue().compareTo(o.getValue());
    }

    public boolean isValid(){
        return getValue() != -1;
    }

    /**
     *
     * @param value The integer which potentially represents a Type
     * @return The matching type ID
     */
    public static LabelId of(Integer value){
        return new AutoValue_LabelId(value);
    }

    /**
     * @return a type id which does not match any type
     */
    public static LabelId invalid(){
        return new AutoValue_LabelId(-1);
    }
}

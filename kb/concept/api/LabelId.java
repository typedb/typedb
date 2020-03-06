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
 * A Type Id
 * A class which represents an id of any SchemaConcept.
 * Also contains a static method for producing IDs from Integers.
 */
public class LabelId implements Comparable<LabelId>, Serializable {
    private static final long serialVersionUID = -1676610785035926909L;

    private final Integer value;

    public LabelId(Integer value) {
        if (value == null) {
            throw new NullPointerException("Null value");
        }
        this.value = value;
    }

    /**
     * @param value The integer which potentially represents a Type
     * @return The matching type ID
     */
    public static LabelId of(Integer value) {
        return new LabelId(value);
    }

    /**
     * @return a type id which does not match any type
     */
    public static LabelId invalid() {
        return new LabelId(-1);
    }

    /**
     * @return Used for indexing purposes and for graql traversals
     */
    @CheckReturnValue
    public Integer getValue() {
        return value;
    }

    @Override
    public int compareTo(LabelId o) {
        return getValue().compareTo(o.getValue());
    }

    public boolean isValid() {
        return getValue() != -1;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof LabelId) {
            LabelId that = (LabelId) o;
            return (this.value.equals(that.getValue()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.value.hashCode();
        return h;
    }
}

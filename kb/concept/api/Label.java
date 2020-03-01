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
import java.util.function.Function;

/**
 * A Label
 * A class which represents the unique label of any SchemaConcept
 * Also contains a static method for producing Labels from Strings.
 */
public class Label implements Comparable<Label>, Serializable {
    private static final long serialVersionUID = 2051578406740868932L;

    private final String value;

    public Label(String value) {
        if (value == null) {
            throw new NullPointerException("Null value");
        }
        this.value = value;
    }

    /**
     * @param value The string which potentially represents a Type
     * @return The matching Type Label
     */
    @CheckReturnValue
    public static Label of(String value) {
        return new Label(value);
    }

    public String getValue() {
        return value;
    }

    /**
     * Rename a Label (does not modify the original Label)
     *
     * @param mapper a function to apply to the underlying type label
     * @return the new type label
     */
    @CheckReturnValue
    public Label map(Function<String, String> mapper) {
        return Label.of(mapper.apply(getValue()));
    }

    @Override
    public int compareTo(Label o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public final String toString() {
        return getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof Label) {
            Label that = (Label) o;
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

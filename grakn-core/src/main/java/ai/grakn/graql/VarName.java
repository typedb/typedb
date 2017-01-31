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

package ai.grakn.graql;

import org.apache.commons.lang.StringUtils;

import java.util.UUID;
import java.util.function.Function;

/**
 * A variable name in a Graql query
 *
 * @author Felix Chapman
 */
public final class VarName {
    private final String value;

    public static VarName of(String value) {
        return new VarName(value);
    }

    public static VarName anon() {
        return new VarName(UUID.randomUUID().toString());
    }

    private VarName(String value) {
        this.value = value;
    }

    /**
     * Get the string name of the variable (without prefixed "$")
     */
    public String getValue() {
        return value;
    }

    /**
     * Rename a variable (does not modify the original {@code VarName})
     * @param mapper a function to apply to the underlying variable name
     * @return the new variable name
     */
    public VarName map(Function<String, String> mapper) {
        return VarName.of(mapper.apply(value));
    }

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    public String shortName() {
        return "$" + StringUtils.left(value, 3);
    }

    public String toString() {
        return "$" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarName varName = (VarName) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}

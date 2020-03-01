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
 */

package grakn.core.graph.core.schema;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;

/**
 * Simple class to represent arbitrary parameters as key-value pairs.
 * Parameters are used in configuration and definitions.
 */
public class Parameter<V> {

    private final String key;
    private final V value;

    public Parameter(String key, V value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "Invalid key");
        this.key = key;
        this.value = value;
    }

    public static <V> Parameter<V> of(String key, V value) {
        return new Parameter(key, value);
    }

    public String key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        if (!getClass().isInstance(oth)) return false;
        Parameter other = (Parameter) oth;
        return key.equals(other.key) && (value == other.value || (value != null && value.equals(other.value)));
    }

    @Override
    public String toString() {
        return key + "->" + value;
    }

}

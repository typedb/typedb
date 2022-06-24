/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.parameters.util;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class Option {

    public static final String PREFIX = "--";

    private final String name;
    private final String value;

    public Option(String name) {
        this(name, null);
    }

    public Option(String name, @Nullable String value) {
        this.name = name;
        this.value = value;
    }

    public String name() {
        return name;
    }

    public boolean hasValue() {
        return value != null;
    }

    public Optional<String> stringValue() {
        return Optional.ofNullable(value);
    }

    @Override
    public String toString() {
        return value == null ? PREFIX + name : PREFIX + name + "=" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Option that = (Option) o;
        return name.equals(that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

}

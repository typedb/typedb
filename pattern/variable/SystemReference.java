/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.pattern.variable;

import graql.lang.pattern.variable.Reference;

import java.util.Objects;

public class SystemReference extends Reference.Name {

    public static String PREFIX = "$/";

    private final int id;
    private final int hash;

    public SystemReference(int id) {
        super("" + id);
        this.id = id;
        this.hash = Objects.hash(SystemReference.class, id);
    }

    public static SystemReference of(int id) {
        return new SystemReference(id);
    }

    @Override
    public String syntax() {
        return PREFIX + name;
    }

    @Override
    public boolean isSystemReference() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SystemReference that = (SystemReference) o;
        return this.id == that.id;
    }

    @Override
    public int hashCode() {
        return hash;
    }
}

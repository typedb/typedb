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

package grakn.core.common.parameters;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

// TODO: Apply this class as a replacement of all "label" and "scope" Strings
//       used throughout the codebase. You should replace all occurrences of
//       String label, @Nullable String scope
public class Label {

    private final String name;
    private final String scope;
    private final int hash;

    private Label(String name, @Nullable String scope) {
        this.name = name;
        this.scope = scope;
        this.hash = Objects.hash(name, scope);
    }

    public static Label of(String name) {
        return new Label(name, null);
    }

    public static Label of(String name, @Nullable String scope) {
        return new Label(name, scope);
    }

    public String name() {
        return name;
    }

    public Optional<String> scope() {
        return Optional.ofNullable(scope);
    }

    public String scopedName() {
        if (scope == null) return name;
        else return scope + ":" + name;
    }

    @Override
    public String toString() {
        return scopedName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Label that = (Label) o;
        return this.name.equals(that.name) && Objects.equals(this.scope, that.scope);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}

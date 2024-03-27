/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

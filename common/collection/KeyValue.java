/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.common.collection;

import java.util.Objects;

public class KeyValue<T extends Comparable<T>, U> implements Comparable<KeyValue<T, U>> {

    T key;
    U value;

    public KeyValue(T key, U value) {
        this.key = key;
        this.value = value;
    }

    public static <T extends Comparable<T>, U> KeyValue<T, U> of(T first, U second) {
        return new KeyValue<>(first, second);
    }

    public T key() {
        return key;
    }

    public U value() {
        return value;
    }

    @Override
    public int compareTo(KeyValue<T, U> other) {
        return key().compareTo(other.key());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValue<?, ?> that = (KeyValue<?, ?>) o;
        return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}

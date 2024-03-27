/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.collection;

import java.util.Objects;

public class KeyValue<T extends Comparable<? super T>, U> implements Comparable<KeyValue<T, U>> {

    private final T key;
    private final U value;

    public KeyValue(T key, U value) {
        this.key = key;
        this.value = value;
    }

    public static <T extends Comparable<? super T>, U> KeyValue<T, U> of(T first, U second) {
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

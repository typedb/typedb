/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.answer;

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.value.Value;

import java.util.Objects;
import java.util.Optional;

public class ValueGroup {
    private final Concept owner;
    private final Optional<Value<?>> value; // note: optionality conveys empty group with invalid value
    private final int hash;

    public ValueGroup(Concept owner, Optional<Value<?>> value) {
        this.owner = owner;
        this.value = value;
        this.hash = Objects.hash(this.owner, this.value);
    }

    public Concept owner() {
        return this.owner;
    }

    public Optional<Value<?>> value() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ValueGroup that = (ValueGroup) obj;
        return this.owner.equals(that.owner) && this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}

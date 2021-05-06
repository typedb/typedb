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

package com.vaticle.typedb.core.concept.answer;

import com.vaticle.typedb.core.concept.Concept;

import java.util.Objects;

public class NumericGroup implements Answer {
    private final Concept owner;
    private final Numeric numeric;
    private final int hash;

    public NumericGroup(Concept owner, Numeric numeric) {
        this.owner = owner;
        this.numeric = numeric;
        this.hash = Objects.hash(this.owner, this.numeric);
    }

    public Concept owner() {
        return this.owner;
    }

    public Numeric numeric() {
        return this.numeric;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NumericGroup a2 = (NumericGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.numeric.equals(a2.numeric);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}

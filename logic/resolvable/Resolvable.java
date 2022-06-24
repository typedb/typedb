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
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Resolvable<T extends Pattern> {

    private T pattern;

    public Resolvable(T pattern) {this.pattern = pattern;}

    public T pattern() {
        return pattern;
    }

    public abstract Optional<ThingVariable> generating();

    public abstract Set<Retrievable> retrieves();

    public boolean isRetrievable() {
        return false;
    }

    public boolean isConcludable() {
        return false;
    }

    public boolean isNegated() {
        return false;
    }

    public com.vaticle.typedb.core.logic.resolvable.Retrievable asRetrievable() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(com.vaticle.typedb.core.logic.resolvable.Retrievable.class));
    }

    public Concludable asConcludable() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Concludable.class));
    }

    public Negated asNegated() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Negated.class));
    }
}

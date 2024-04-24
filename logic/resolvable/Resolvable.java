/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Resolvable<T extends Pattern> {

    private final T pattern;

    public Resolvable(T pattern) {this.pattern = pattern;}

    public T pattern() {
        return pattern;
    }

    public abstract Set<Variable> generating();

    public abstract Set<Retrievable> retrieves();

    public abstract Set<Variable> variables();

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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[ " + pattern.toString() + " ]";
    }
}

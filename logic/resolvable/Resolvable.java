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
 */

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Pattern;
import grakn.core.pattern.variable.Variable;
import graql.lang.pattern.variable.Reference;

import java.util.Optional;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Resolvable<T extends Pattern> {

    private T pattern;

    public Resolvable(T pattern) {this.pattern = pattern;}

    public T pattern() {
        return pattern;
    }

    public abstract Optional<Variable> generating();

    public abstract Set<Reference.Name> variableNames();

    public boolean isRetrievable() {
        return false;
    }

    public boolean isConcludable() {
        return false;
    }

    public boolean isNegated() {
        return false;
    }

    public Retrievable asRetrievable() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Retrievable.class));
    }

    public Concludable asConcludable() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Concludable.class));
    }

    public Negated asNegated() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Negated.class));
    }
}

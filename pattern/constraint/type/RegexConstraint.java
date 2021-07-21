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

package com.vaticle.typedb.core.pattern.constraint.type;

import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.SubString.LIKE;

public class RegexConstraint extends TypeConstraint {

    private final java.util.regex.Pattern regex;
    private final int hash;

    public RegexConstraint(TypeVariable owner, java.util.regex.Pattern regex) {
        super(owner, set());
        this.regex = regex;
        this.hash = Objects.hash(RegexConstraint.class, this.owner, this.regex.pattern());
    }

    static RegexConstraint of(TypeVariable owner, com.vaticle.typeql.lang.pattern.constraint.TypeConstraint.Regex constraint) {
        return new RegexConstraint(owner, constraint.regex());
    }

    static RegexConstraint of(TypeVariable owner, RegexConstraint clone) {
        return new RegexConstraint(owner, clone.regex());
    }

    public java.util.regex.Pattern regex() {
        return regex;
    }

    @Override
    public void addTo(GraphTraversal traversal) {
        traversal.regex(owner.id(), regex.pattern());
    }

    @Override
    public boolean isRegex() {
        return true;
    }

    @Override
    public RegexConstraint asRegex() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegexConstraint that = (RegexConstraint) o;
        return (this.owner.equals(that.owner) && this.regex.pattern().equals(that.regex.pattern()));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + LIKE + SPACE + regex.toString();
    }

    @Override
    public RegexConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).regex(regex);
    }
}

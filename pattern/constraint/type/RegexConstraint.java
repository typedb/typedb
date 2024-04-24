/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
    public void addTo(GraphTraversal.Thing traversal) {
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
    public RegexConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).regex(regex);
    }
}

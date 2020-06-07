/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.reasoner.atom.predicate;

import grakn.core.graql.reasoner.atom.AtomicBase;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Objects;

/**
 * AtomicBase extension serving as base class for predicate implementations.
 *
 * @param <T> the type of the predicate on a concept
 */
public abstract class Predicate<T> extends AtomicBase {

    private final T predicate;

    public Predicate(Variable varName, Statement pattern, T predicate, ReasonerQuery parentQuery) {
        super(parentQuery, varName, pattern);
        this.predicate = predicate;
    }

    public T getPredicate() { return predicate; }
    public abstract String getPredicateValue();

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Predicate that = (Predicate) obj;
        return this.getVarName().equals(that.getVarName())
                && this.getPredicate().equals(that.getPredicate());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public boolean isSubsumedBy(Atomic atom) { return this.isAlphaEquivalent(atom); }

    @Override
    public int hashCode() {
        return Objects.hash(getVarName(), getPredicate());
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }
}
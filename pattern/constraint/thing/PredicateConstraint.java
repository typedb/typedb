/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.constraint.common.Predicate;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class PredicateConstraint extends ThingConstraint implements AlphaEquivalent<PredicateConstraint> {

    final Predicate<?> predicate;
    private final int hash;

    public PredicateConstraint(ThingVariable owner, Predicate<?> predicate) {
        super(owner, predicate.variables());
        this.predicate = predicate;
        this.hash = Objects.hash(this.getClass(), owner, this.predicate);
        predicate.variables().forEach(v -> v.constraining(this));
    }

    static PredicateConstraint of(ThingVariable owner,
                                  com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Predicate predicateConstraint,
                                  VariableRegistry register) {
        return new PredicateConstraint(owner, Predicate.of(predicateConstraint.predicate(), register));
    }

    static PredicateConstraint of(ThingVariable owner, PredicateConstraint toClone, VariableCloner cloner) {
        return new PredicateConstraint(owner, Predicate.of(toClone.predicate(), cloner));
    }

    @Override
    public boolean isPredicate() {
        return true;
    }

    @Override
    public PredicateConstraint asPredicate() {
        return this;
    }

    @Override
    public String toString() {
        return owner.toString() + TypeQLToken.Char.SPACE + predicate.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PredicateConstraint that = (PredicateConstraint) o;
        return this.owner.equals(that.owner) && this.predicate.equals(that.predicate);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public Predicate<?> predicate() {
        return predicate;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        if (predicate.isConstant()) {
            Predicate.Constant<?> asConstant = predicate.asConstant();
            if (asConstant.valueType().equals(Encoding.ValueType.BOOLEAN)) {
                traversal.predicateThing(owner.id(), predicate.predicate().asEquality(), asConstant.asBoolean().value());
            } else if (asConstant.valueType().equals(Encoding.ValueType.LONG)) {
                traversal.predicateThing(owner.id(), predicate.predicate().asEquality(), asConstant.asLong().value());
            } else if (asConstant.valueType().equals(Encoding.ValueType.DOUBLE)) {
                traversal.predicateThing(owner.id(), predicate.predicate().asEquality(), asConstant.asDouble().value());
            } else if (asConstant.valueType().equals(Encoding.ValueType.DATETIME)) {
                traversal.predicateThing(owner.id(), predicate.predicate().asEquality(), asConstant.asDateTime().value());
            } else if (asConstant.valueType().equals(Encoding.ValueType.STRING) && asConstant.predicate().isEquality()) {
                traversal.predicateThing(owner.id(), predicate.predicate().asEquality(), asConstant.asString().value());
            } else if (asConstant.valueType().equals(Encoding.ValueType.STRING) && asConstant.predicate().isSubString()) {
                traversal.predicateThing(owner.id(), predicate.predicate().asSubString(), asConstant.asString().value());
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else if (predicate.isThingVar()) {
            traversal.predicateThingThing(owner.id(), predicate.predicate().asEquality(), predicate.asThingVar().value().id());
        } else if (predicate.isValueVar()) {
            traversal.predicateThingValue(owner.id(), predicate.predicate().asEquality(), predicate.asValueVar().value().id());
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public PredicateConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).predicate(predicate.clone(cloner));
    }

    @Override
    public FunctionalIterator<AlphaEquivalence> alphaEquals(PredicateConstraint that) {
        return owner.alphaEquals(that.owner)
                .flatMap(a -> predicate.alphaEquals(that.predicate).flatMap(a::extendIfCompatible));
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;

import java.util.Collections;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.concatToSet;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class ThingConstraint extends Constraint {

    protected final ThingVariable owner;
    private final Set<Variable> variables;

    ThingConstraint(ThingVariable owner, Set<Variable> additionalVariables) {
        this.owner = owner;
        variables = Collections.unmodifiableSet(concatToSet(additionalVariables, set(owner)));
    }

    public static ThingConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint constraint,
                                     VariableRegistry registry) {
        if (constraint.isIID()) return IIDConstraint.of(owner, constraint.asIID());
        else if (constraint.isIsa()) return IsaConstraint.of(owner, constraint.asIsa(), registry);
        else if (constraint.isPredicate()) return PredicateConstraint.of(owner, constraint.asPredicate(), registry);
        else if (constraint.isRelation()) return RelationConstraint.of(owner, constraint.asRelation(), registry);
        else if (constraint.isHas()) return HasConstraint.of(owner, constraint.asHas(), registry);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static ThingConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ConceptConstraint constraint,
                                     VariableRegistry registry) {
        if (constraint.isIs()) return IsConstraint.of(owner, constraint.asIs(), registry);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static ThingConstraint of(ThingVariable owner, ThingConstraint clone, VariableCloner cloner) {
        if (clone.isIID()) return IIDConstraint.of(owner, clone.asIID());
        else if (clone.isIsa()) return IsaConstraint.of(owner, clone.asIsa(), cloner);
        else if (clone.isPredicate()) return PredicateConstraint.of(owner, clone.asPredicate(), cloner);
        else if (clone.isRelation()) return RelationConstraint.of(owner, clone.asRelation(), cloner);
        else if (clone.isHas()) return HasConstraint.of(owner, clone.asHas(), cloner);
        else if (clone.isIs()) return IsConstraint.of(owner, clone.asIs(), cloner);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public ThingVariable owner() {
        return owner;
    }

    @Override
    public Set<Variable> variables() {
        return variables;
    }

    @Override
    public boolean isThing() {
        return true;
    }

    @Override
    public ThingConstraint asThing() {
        return this;
    }

    public boolean isIID() {
        return false;
    }

    public boolean isIsa() {
        return false;
    }

    public boolean isIs() {
        return false;
    }

    public boolean isPredicate() {
        return false;
    }

    public boolean isRelation() {
        return false;
    }

    public boolean isHas() {
        return false;
    }

    public IIDConstraint asIID() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(IIDConstraint.class));
    }

    public IsaConstraint asIsa() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(IsaConstraint.class));
    }

    public IsConstraint asIs() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(IsConstraint.class));
    }

    public PredicateConstraint asPredicate() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(PredicateConstraint.class));
    }

    public RelationConstraint asRelation() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(RelationConstraint.class));
    }

    public HasConstraint asHas() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(HasConstraint.class));
    }
}

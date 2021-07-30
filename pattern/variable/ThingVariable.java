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

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.ILLEGAL_DERIVED_THING_CONSTRAINT_ISA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.MULTIPLE_THING_CONSTRAINT_IID;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.MULTIPLE_THING_CONSTRAINT_ISA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.MULTIPLE_THING_CONSTRAINT_RELATION;

public class ThingVariable extends Variable implements AlphaEquivalent<ThingVariable> {

    private IIDConstraint iidConstraint;
    private IsaConstraint isaConstraint;
    private RelationConstraint relationConstraint;
    private final Set<IsConstraint> isConstraints;
    private final Set<HasConstraint> hasConstraints;
    private final Set<ValueConstraint<?>> valueConstraints;
    private final Set<ThingConstraint> constraints;
    private final Set<Constraint> constraining;

    public ThingVariable(Identifier.Variable identifier) {
        super(identifier);
        this.isConstraints = new HashSet<>();
        this.valueConstraints = new HashSet<>();
        this.hasConstraints = new HashSet<>();
        this.constraints = new HashSet<>();
        this.constraining = new HashSet<>();
    }

    ThingVariable constrainThing(List<com.vaticle.typeql.lang.pattern.constraint.ThingConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> {
            if (constraint.isIsa() && constraint.asIsa().isDerived() && !registry.allowsDerived()) {
                throw TypeDBException.of(ILLEGAL_DERIVED_THING_CONSTRAINT_ISA, id(), constraint.asIsa().type());
            }
            this.constrain(ThingConstraint.of(this, constraint, registry));
        });
        return this;
    }

    ThingVariable constrainConcept(List<com.vaticle.typeql.lang.pattern.constraint.ConceptConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, registry)));
        return this;
    }

    void constrainClone(ThingVariable toClone, VariableCloner cloner) {
        toClone.constraints().forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, cloner)));
    }

    public ThingVariable clone() {
        ThingVariable clone = new ThingVariable(id());
        clone.addResolvedTypes(resolvedTypes());
        return clone;
    }

    public static ThingVariable of(Identifier.Variable identifier) {
        return new ThingVariable(identifier);
    }

    @Override
    public Identifier.Variable.Retrievable id() {
        return identifier.asRetrievable();
    }

    @Override
    public void addTo(GraphTraversal traversal) {
        // TODO: create vertex properties first, then the vertex itself, then edges
        //       that way, we can make properties to be 'final' objects that are
        //       included in equality and hashCode of vertices
        if (!resolvedTypes().isEmpty()) traversal.types(id(), resolvedTypes());
        constraints().forEach(constraint -> constraint.addTo(traversal));
    }

    private void constrain(ThingConstraint constraint) {
        constraints.add(constraint);
        if (constraint.isIID()) {
            if (iidConstraint != null && !iidConstraint.equals(constraint)) {
                throw TypeDBException.of(MULTIPLE_THING_CONSTRAINT_IID, id());
            }
            iidConstraint = constraint.asIID();
        } else if (constraint.isIsa()) {
            if (isaConstraint != null && !isaConstraint.equals(constraint)) {
                throw TypeDBException.of(MULTIPLE_THING_CONSTRAINT_ISA, id(), constraint.asIsa().type(), isaConstraint.type());
            }
            isaConstraint = constraint.asIsa();
        } else if (constraint.isRelation()) {
            if (relationConstraint != null && !relationConstraint.equals(constraint)) {
                throw TypeDBException.of(MULTIPLE_THING_CONSTRAINT_RELATION, id());
            }
            relationConstraint = constraint.asRelation();
        } else if (constraint.isIs()) isConstraints.add(constraint.asIs());
        else if (constraint.isHas()) hasConstraints.add(constraint.asHas());
        else if (constraint.isValue()) valueConstraints.add(constraint.asValue());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public void constraining(Constraint constraint) {
        constraining.add(constraint);
    }

    public Optional<IIDConstraint> iid() {
        return Optional.ofNullable(iidConstraint);
    }

    public IIDConstraint iid(ByteArray iid) {
        IIDConstraint iidConstraint = new IIDConstraint(this, iid);
        constrain(iidConstraint);
        return iidConstraint;
    }

    public Optional<IsaConstraint> isa() {
        return Optional.ofNullable(isaConstraint);
    }

    public Optional<RelationConstraint> relation() {
        return Optional.ofNullable(relationConstraint);
    }

    public IsaConstraint isa(TypeVariable type, boolean isExplicit) {
        IsaConstraint isaConstraint = new IsaConstraint(this, type, isExplicit);
        constrain(isaConstraint);
        return isaConstraint;
    }

    public Set<IsConstraint> is() {
        return isConstraints;
    }

    public IsConstraint is(ThingVariable variable) {
        IsConstraint isConstraint = new IsConstraint(this, variable);
        constrain(isConstraint);
        return isConstraint;
    }

    public Set<ValueConstraint<?>> value() {
        return valueConstraints;
    }

    public ValueConstraint.Long valueLong(TypeQLToken.Predicate.Equality comparator, long value) {
        ValueConstraint.Long valueLongConstraint = new ValueConstraint.Long(this, comparator, value);
        constrain(valueLongConstraint);
        return valueLongConstraint;
    }

    public ValueConstraint.Double valueDouble(TypeQLToken.Predicate.Equality comparator, double value) {
        ValueConstraint.Double valueDoubleConstraint = new ValueConstraint.Double(this, comparator, value);
        constrain(valueDoubleConstraint);
        return valueDoubleConstraint;
    }

    public ValueConstraint.Boolean valueBoolean(TypeQLToken.Predicate.Equality comparator, boolean value) {
        ValueConstraint.Boolean valueBooleanConstraint = new ValueConstraint.Boolean(this, comparator, value);
        constrain(valueBooleanConstraint);
        return valueBooleanConstraint;
    }

    public ValueConstraint.String valueString(TypeQLToken.Predicate comparator, String value) {
        ValueConstraint.String valueStringConstraint = new ValueConstraint.String(this, comparator, value);
        constrain(valueStringConstraint);
        return valueStringConstraint;
    }

    public ValueConstraint.DateTime valueDateTime(TypeQLToken.Predicate.Equality comparator, LocalDateTime value) {
        ValueConstraint.DateTime valueDateTimeConstraint = new ValueConstraint.DateTime(this, comparator, value);
        constrain(valueDateTimeConstraint);
        return valueDateTimeConstraint;
    }

    public ValueConstraint.Variable valueVariable(TypeQLToken.Predicate.Equality comparator, ThingVariable variable) {
        ValueConstraint.Variable valueVarConstraint = new ValueConstraint.Variable(this, comparator, variable);
        constrain(valueVarConstraint);
        return valueVarConstraint;
    }

    public RelationConstraint relation(LinkedHashSet<RelationConstraint.RolePlayer> rolePlayers) {
        RelationConstraint relationConstraint = new RelationConstraint(this, rolePlayers);
        constrain(relationConstraint);
        return relationConstraint;
    }

    public Set<HasConstraint> has() {
        return hasConstraints;
    }

    public HasConstraint has(ThingVariable attribute) {
        HasConstraint hasConstraint = new HasConstraint(this, attribute);
        constrain(hasConstraint);
        return hasConstraint;
    }

    @Override
    public Set<ThingConstraint> constraints() {
        return constraints;
    }

    @Override
    public Set<Constraint> constraining() {
        return constraining;
    }

    @Override
    public boolean isThing() {
        return true;
    }

    @Override
    public ThingVariable asThing() {
        return this;
    }

    @Override
    public AlphaEquivalence alphaEquals(ThingVariable that) {
        return AlphaEquivalence.valid()
                .validIf(id().isName() == that.id().isName())
                .validIf(this.resolvedTypes().equals(that.resolvedTypes()))
                .validIfAlphaEqual(this.isaConstraint, that.isaConstraint)
                .validIfAlphaEqual(this.relationConstraint, that.relationConstraint)
                .validIfAlphaEqual(this.hasConstraints, that.hasConstraints)
                .validIfAlphaEqual(this.valueConstraints, that.valueConstraints)
                .addMapping(this, that);
    }
}

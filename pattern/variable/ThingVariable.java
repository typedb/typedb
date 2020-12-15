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

package grakn.core.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.traversal.common.Identifier;
import graql.lang.common.GraqlToken;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_THING_CONSTRAINT_IID;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_THING_CONSTRAINT_ISA;
import static graql.lang.common.GraqlToken.Char.COMMA;
import static graql.lang.common.GraqlToken.Char.SPACE;

public class ThingVariable extends Variable implements AlphaEquivalent<ThingVariable> {

    private IIDConstraint iidConstraint;
    private IsaConstraint isaConstraint;
    private final Set<IsConstraint> isConstraints;
    private final Set<RelationConstraint> relationConstraints;
    private final Set<HasConstraint> hasConstraints;
    private final Set<ValueConstraint<?>> valueConstraints;
    private final Set<ThingConstraint> constraints;

    ThingVariable(Identifier.Variable identifier) {
        super(identifier);
        this.isConstraints = new HashSet<>();
        this.valueConstraints = new HashSet<>();
        this.relationConstraints = new HashSet<>();
        this.hasConstraints = new HashSet<>();
        this.constraints = new HashSet<>();
    }

    ThingVariable constrainThing(List<graql.lang.pattern.constraint.ThingConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, registry)));
        return this;
    }

    ThingVariable constrainConcept(List<graql.lang.pattern.constraint.ConceptConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, registry)));
        return this;
    }

    public static ThingVariable of(Identifier.Variable identifier) {
        return new ThingVariable(identifier);
    }

    private void constrain(ThingConstraint constraint) {
        constraints.add(constraint);
        if (constraint.isIID()) {
            if (iidConstraint != null && !iidConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_THING_CONSTRAINT_IID, identifier());
            }
            iidConstraint = constraint.asIID();
        } else if (constraint.isIsa()) {
            if (isaConstraint != null && !isaConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_THING_CONSTRAINT_ISA, identifier());
            }
            isaConstraint = constraint.asIsa();
        } else if (constraint.isIs()) isConstraints.add(constraint.asIs());
        else if (constraint.isRelation()) relationConstraints.add(constraint.asRelation());
        else if (constraint.isHas()) hasConstraints.add(constraint.asHas());
        else if (constraint.isValue()) valueConstraints.add(constraint.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<IIDConstraint> iid() {
        return Optional.ofNullable(iidConstraint);
    }

    public Optional<IsaConstraint> isa() {
        return Optional.ofNullable(isaConstraint);
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

    public ValueConstraint.Long valueLong(GraqlToken.Predicate.Equality comparator, long value) {
        ValueConstraint.Long valueLongConstraint = new ValueConstraint.Long(this, comparator, value);
        constrain(valueLongConstraint);
        return valueLongConstraint;
    }

    public ValueConstraint.Double valueDouble(GraqlToken.Predicate.Equality comparator, double value) {
        ValueConstraint.Double valueDoubleConstraint = new ValueConstraint.Double(this, comparator, value);
        constrain(valueDoubleConstraint);
        return valueDoubleConstraint;
    }

    public ValueConstraint.Boolean valueBoolean(GraqlToken.Predicate.Equality comparator, boolean value) {
        ValueConstraint.Boolean valueBooleanConstraint = new ValueConstraint.Boolean(this, comparator, value);
        constrain(valueBooleanConstraint);
        return valueBooleanConstraint;
    }

    public ValueConstraint.String valueString(GraqlToken.Predicate comparator, String value) {
        ValueConstraint.String valueStringConstraint = new ValueConstraint.String(this, comparator, value);
        constrain(valueStringConstraint);
        return valueStringConstraint;
    }

    public ValueConstraint.DateTime valueDateTime(GraqlToken.Predicate.Equality comparator, LocalDateTime value) {
        ValueConstraint.DateTime valueDateTimeConstraint = new ValueConstraint.DateTime(this, comparator, value);
        constrain(valueDateTimeConstraint);
        return valueDateTimeConstraint;
    }

    public Set<RelationConstraint> relation() {
        return relationConstraints;
    }

    public RelationConstraint relation(List<RelationConstraint.RolePlayer> rolePlayers) {
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
    public boolean isThing() {
        return true;
    }

    @Override
    public ThingVariable asThing() {
        return this;
    }

    @Override
    public String toString() {

        StringBuilder syntax = new StringBuilder();

        if (reference().isName()) syntax.append(reference()).append(SPACE);

        syntax.append(Stream.of(relationConstraints, set(isaConstraint), hasConstraints, valueConstraints, isConstraints)
                .flatMap(Collection::stream).map(ThingConstraint::toString)
                .collect(Collectors.joining("" + COMMA + SPACE)));

        if (iidConstraint != null) syntax.append(COMMA).append(SPACE).append(iidConstraint);

        return syntax.toString();
    }

    @Override
    public AlphaEquivalence alphaEquals(ThingVariable that) {
        return AlphaEquivalence.valid()
                .validIf(identifier().isNamedReference() == that.identifier().isNamedReference())
                .validIf(this.typeHints().equals(that.typeHints()))
                .validIfAlphaEqual(this.isaConstraint, that.isaConstraint)
                .validIfAlphaEqual(this.relationConstraints, that.relationConstraints)
                .validIfAlphaEqual(this.hasConstraints, that.hasConstraints)
                .validIfAlphaEqual(this.valueConstraints, that.valueConstraints)
                .addMapping(this, that);
    }

}

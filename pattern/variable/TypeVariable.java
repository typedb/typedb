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
import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.type.AbstractConstraint;
import grakn.core.pattern.constraint.type.IsConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
import grakn.core.pattern.constraint.type.ValueTypeConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.traversal.common.Identifier;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.constraint.ConceptConstraint;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_TYPE_CONSTRAINT_LABEL;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_TYPE_CONSTRAINT_REGEX;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_TYPE_CONSTRAINT_SUB;
import static grakn.core.common.exception.ErrorMessage.Pattern.MULTIPLE_TYPE_CONSTRAINT_VALUE_TYPE;
import static graql.lang.common.GraqlToken.Char.COMMA;
import static graql.lang.common.GraqlToken.Char.SPACE;

public class TypeVariable extends Variable implements AlphaEquivalent<TypeVariable> {

    private LabelConstraint labelConstraint;
    private AbstractConstraint abstractConstraint;
    private ValueTypeConstraint valueTypeConstraint;
    private RegexConstraint regexConstraint;
    private SubConstraint subConstraint;

    private final Set<OwnsConstraint> ownsConstraints;
    private final Set<PlaysConstraint> playsConstraints;
    private final Set<RelatesConstraint> relatesConstraints;
    private final Set<IsConstraint> isConstraints;
    private final Set<TypeConstraint> constraints;

    public TypeVariable(Identifier.Variable identifier) {
        super(identifier);
        ownsConstraints = new HashSet<>();
        playsConstraints = new HashSet<>();
        relatesConstraints = new HashSet<>();
        isConstraints = new HashSet<>();
        constraints = new HashSet<>();
    }

    public static TypeVariable of(Identifier.Variable identifier) {
        return new TypeVariable(identifier);
    }

    TypeVariable constrainType(List<graql.lang.pattern.constraint.TypeConstraint> constraints, VariableRegistry register) {
        constraints.forEach(constraint -> this.constrain(TypeConstraint.of(this, constraint, register)));
        return this;
    }

    Variable constrainConcept(List<ConceptConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(TypeConstraint.of(this, constraint, registry)));
        return this;
    }

    public void constrain(TypeConstraint constraint) {
        constraints.add(constraint);
        if (constraint.isLabel()) {
            if (labelConstraint != null && !labelConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_TYPE_CONSTRAINT_LABEL, identifier());
            }
            labelConstraint = constraint.asLabel();
        } else if (constraint.isValueType()) {
            if (valueTypeConstraint != null && !valueTypeConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_TYPE_CONSTRAINT_VALUE_TYPE, identifier());
            }
            valueTypeConstraint = constraint.asValueType();
        } else if (constraint.isRegex()) {
            if (regexConstraint != null && !regexConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_TYPE_CONSTRAINT_REGEX, identifier());
            }
            regexConstraint = constraint.asRegex();
        } else if (constraint.isAbstract()) abstractConstraint = constraint.asAbstract();
        else if (constraint.isSub()) {
            if (subConstraint != null && !subConstraint.equals(constraint)) {
                throw GraknException.of(MULTIPLE_TYPE_CONSTRAINT_SUB, identifier());
            }
            subConstraint = constraint.asSub();
        } else if (constraint.isOwns()) ownsConstraints.add(constraint.asOwns());
        else if (constraint.isPlays()) playsConstraints.add(constraint.asPlays());
        else if (constraint.isRelates()) relatesConstraints.add(constraint.asRelates());
        else if (constraint.isIs()) isConstraints.add(constraint.asIs());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<LabelConstraint> label() {
        return Optional.ofNullable(labelConstraint);
    }

    public LabelConstraint label(Label label) {
        LabelConstraint labelConstraint = new LabelConstraint(this, label);
        constrain(labelConstraint);
        return labelConstraint;
    }

    public Optional<AbstractConstraint> abstractConstraint() {
        return Optional.ofNullable(abstractConstraint);
    }

    public Optional<ValueTypeConstraint> valueType() {
        return Optional.ofNullable(valueTypeConstraint);
    }

    public ValueTypeConstraint valueType(GraqlArg.ValueType valueType) {
        ValueTypeConstraint valueTypeConstraint = new ValueTypeConstraint(this, valueType);
        constrain(valueTypeConstraint);
        return valueTypeConstraint;
    }

    public Optional<RegexConstraint> regex() {
        return Optional.ofNullable(regexConstraint);
    }

    public Optional<SubConstraint> sub() {
        return Optional.ofNullable(subConstraint);
    }

    public SubConstraint sub(TypeVariable type, boolean isExplicit) {
        SubConstraint subConstraint = new SubConstraint(this, type, isExplicit);
        constrain(subConstraint);
        return subConstraint;
    }

    public Set<OwnsConstraint> owns() {
        return ownsConstraints;
    }

    public OwnsConstraint owns(TypeVariable attributeType, @Nullable TypeVariable overriddenAttributeType, boolean isKey) {
        OwnsConstraint ownsConstraint = new OwnsConstraint(this, attributeType, overriddenAttributeType, isKey);
        constrain(ownsConstraint);
        return ownsConstraint;
    }

    public Set<PlaysConstraint> plays() {
        return playsConstraints;
    }

    public PlaysConstraint plays(@Nullable TypeVariable relationType, TypeVariable roleType, @Nullable TypeVariable overriddenRoleType) {
        PlaysConstraint playsConstraint = new PlaysConstraint(this, relationType, roleType, overriddenRoleType);
        constrain(playsConstraint);
        return playsConstraint;
    }

    public Set<RelatesConstraint> relates() {
        return relatesConstraints;
    }

    public RelatesConstraint relates(TypeVariable roleType, @Nullable TypeVariable overriddenRoleType) {
        RelatesConstraint relatesConstraint = new RelatesConstraint(this, roleType, overriddenRoleType);
        constrain(relatesConstraint);
        return relatesConstraint;
    }

    public Set<IsConstraint> is() {
        return isConstraints;
    }

    public IsConstraint is(TypeVariable variable) {
        IsConstraint isConstraint = new IsConstraint(this, variable);
        constrain(isConstraint);
        return isConstraint;
    }

    @Override
    public Set<TypeConstraint> constraints() {
        return constraints;
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeVariable asType() {
        return this;
    }

    @Override
    public String toString() {

        StringBuilder syntax = new StringBuilder();
        if (!reference().isLabel()) {
            syntax.append(reference());
            if (labelConstraint != null) syntax.append(SPACE).append(labelConstraint.toString());
        } else {
            syntax.append(labelConstraint.label());
        }

        if (constraints.size() > 1 || labelConstraint == null) syntax.append(SPACE);

        syntax.append(Stream.of(set(subConstraint), set(abstractConstraint), ownsConstraints, relatesConstraints,
                                playsConstraints, set(valueTypeConstraint), set(regexConstraint), isConstraints)
                              .flatMap(Set::stream).filter(Objects::nonNull).map(TypeConstraint::toString)
                              .collect(Collectors.joining("" + COMMA + SPACE)));

        return syntax.toString();
    }

    public String referenceSyntax() {
        if (reference().isLabel()) return labelConstraint.label();
        else if (reference().isName() || reference().isAnonymous()) return reference().toString();
        else throw new RuntimeException("Unhandled reference type.");
    }

    @Override
    public AlphaEquivalence alphaEquals(TypeVariable that) {
        return AlphaEquivalence.valid()
                .validIf(identifier().isNamedReference() == that.identifier().isNamedReference())
                .validIfAlphaEqual(labelConstraint, that.labelConstraint)
                .validIfAlphaEqual(valueTypeConstraint, that.valueTypeConstraint)
                .addMapping(this, that);
    }
}

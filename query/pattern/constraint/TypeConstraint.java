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

package grakn.core.query.pattern.constraint;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.type.AttributeType;
import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.VariableRegistry;
import graql.lang.pattern.Pattern;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Query.INVALID_CASTING;

public abstract class TypeConstraint extends Constraint {

    final TypeVariable owner;

    private TypeConstraint(TypeVariable owner) {
        if (owner == null) throw new NullPointerException("Null owner");
        this.owner = owner;
    }

    public static TypeConstraint of(final TypeVariable owner,
                                    final graql.lang.pattern.constraint.TypeConstraint constraint,
                                    final VariableRegistry registry) {
        if (constraint.isLabel()) return TypeConstraint.Label.of(owner, constraint.asLabel());
        else if (constraint.isSub()) return TypeConstraint.Sub.of(owner, constraint.asSub(), registry);
        else if (constraint.isAbstract()) return TypeConstraint.Abstract.of(owner);
        else if (constraint.isValueType()) return TypeConstraint.ValueType.of(owner, constraint.asValueType());
        else if (constraint.isRegex()) return TypeConstraint.Regex.of(owner, constraint.asRegex());
        else if (constraint.isThen()) return TypeConstraint.Then.of(owner, constraint.asThen());
        else if (constraint.isWhen()) return TypeConstraint.When.of(owner, constraint.asWhen());
        else if (constraint.isOwns()) return TypeConstraint.Owns.of(owner, constraint.asOwns(), registry);
        else if (constraint.isPlays()) return TypeConstraint.Plays.of(owner, constraint.asPlays(), registry);
        else if (constraint.isRelates()) return TypeConstraint.Relates.of(owner, constraint.asRelates(), registry);
        else throw new GraknException(ILLEGAL_STATE);
    }

    @Override
    public TypeVariable owner() {
        return owner;
    }

    @Override
    public Set<TypeVariable> variables() {
        return set();
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeConstraint asType() {
        return this;
    }

    public boolean isLabel() {
        return false;
    }

    public boolean isSub() {
        return false;
    }

    public boolean isAbstract() {
        return false;
    }

    public boolean isValueType() {
        return false;
    }

    public boolean isRegex() {
        return false;
    }

    public boolean isThen() {
        return false;
    }

    public boolean isWhen() {
        return false;
    }

    public boolean isOwns() {
        return false;
    }

    public boolean isPlays() {
        return false;
    }

    public boolean isRelates() {
        return false;
    }

    public TypeConstraint.Label asLabel() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Label.class)));
    }

    public TypeConstraint.Sub asSub() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Sub.class)));
    }

    public TypeConstraint.Abstract asAbstract() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Abstract.class)));
    }

    public TypeConstraint.ValueType asValueType() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(ValueType.class)));
    }

    public TypeConstraint.Regex asRegex() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Regex.class)));
    }

    public TypeConstraint.Then asThen() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Then.class)));
    }

    public TypeConstraint.When asWhen() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(When.class)));
    }

    public TypeConstraint.Owns asOwns() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Owns.class)));
    }

    public TypeConstraint.Plays asPlays() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Plays.class)));
    }

    public TypeConstraint.Relates asRelates() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Relates.class)));
    }

    public static class Label extends TypeConstraint {

        private final String label;
        private final String scope;
        private final int hash;

        private Label(TypeVariable owner, @Nullable String scope, String label) {
            super(owner);
            if (label == null) throw new NullPointerException("Null label");
            this.scope = scope;
            this.label = label;
            this.hash = Objects.hash(Label.class, this.owner, this.scope, this.label);
        }

        public static Label of(final TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.Label constraint) {
            return new Label(owner, constraint.scope().orElse(null), constraint.label());
        }

        public Optional<String> scope() {
            return Optional.ofNullable(scope);
        }

        public String label() {
            return label;
        }

        public String scopedLabel() {
            return (scope != null ? scope + ":" : "") + label;
        }

        @Override
        public boolean isLabel() {
            return true;
        }

        @Override
        public TypeConstraint.Label asLabel() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Label that = (Label) o;
            return (this.owner.equals(that.owner) &&
                    this.label.equals(that.label) &&
                    Objects.equals(this.scope, that.scope));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Sub extends TypeConstraint {

        private final TypeVariable type;
        private final boolean isExplicit;
        private final int hash;

        private Sub(TypeVariable owner, TypeVariable type, boolean isExplicit) {
            super(owner);
            if (type == null) throw new NullPointerException("Null superType");
            this.type = type;
            this.isExplicit = isExplicit;
            this.hash = Objects.hash(Sub.class, this.owner, this.type, this.isExplicit);
        }

        public static Sub of(final TypeVariable owner,
                             final graql.lang.pattern.constraint.TypeConstraint.Sub constraint,
                             final VariableRegistry registry) {
            return new Sub(owner, registry.register(constraint.type()), constraint.isExplicit());
        }

        public TypeVariable type() {
            return type;
        }

        @Override
        public Set<TypeVariable> variables() {
            return set(type);
        }

        @Override
        public boolean isSub() {
            return true;
        }

        @Override
        public TypeConstraint.Sub asSub() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sub that = (Sub) o;
            return (this.owner.equals(that.owner) &&
                    this.type.equals(that.type) &&
                    this.isExplicit == that.isExplicit);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Abstract extends TypeConstraint {

        private final int hash;

        private Abstract(TypeVariable owner) {
            super(owner);
            this.hash = Objects.hash(Abstract.class, this.owner);
        }

        public static Abstract of(TypeVariable owner) {
            return new Abstract(owner);
        }

        @Override
        public Set<TypeVariable> variables() {
            return set();
        }

        @Override
        public boolean isAbstract() {
            return true;
        }

        @Override
        public TypeConstraint.Abstract asAbstract() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Abstract that = (Abstract) o;
            return this.owner.equals(that.owner);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class ValueType extends TypeConstraint {

        private final AttributeType.ValueType valueType;
        private final int hash;

        private ValueType(TypeVariable owner, AttributeType.ValueType valueType) {
            super(owner);
            this.valueType = valueType;
            this.hash = Objects.hash(ValueType.class, this.owner, this.valueType);
        }

        public static ValueType of(final TypeVariable owner,
                                   graql.lang.pattern.constraint.TypeConstraint.ValueType constraint) {
            return new ValueType(owner, AttributeType.ValueType.of(constraint.valueType()));
        }

        public AttributeType.ValueType valueType() {
            return valueType;
        }

        @Override
        public Set<TypeVariable> variables() {
            return set();
        }

        @Override
        public boolean isValueType() {
            return true;
        }

        @Override
        public TypeConstraint.ValueType asValueType() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ValueType that = (ValueType) o;
            return (this.owner.equals(that.owner) && this.valueType.equals(that.valueType));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Regex extends TypeConstraint {

        private final java.util.regex.Pattern regex;
        private final int hash;

        private Regex(TypeVariable owner, java.util.regex.Pattern regex) {
            super(owner);
            this.regex = regex;
            this.hash = Objects.hash(Regex.class, this.owner, this.regex.pattern());
        }

        public static Regex of(final TypeVariable owner,
                               graql.lang.pattern.constraint.TypeConstraint.Regex constraint) {
            return new Regex(owner, constraint.regex());
        }

        public java.util.regex.Pattern regex() {
            return regex;
        }

        @Override
        public Set<TypeVariable> variables() {
            return set();
        }

        @Override
        public boolean isRegex() {
            return true;
        }

        @Override
        public TypeConstraint.Regex asRegex() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Regex that = (Regex) o;
            return (this.owner.equals(that.owner) && this.regex.pattern().equals(that.regex.pattern()));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    // TODO: Move this out of TypeConstraint and create its own class
    public static class Then extends TypeConstraint {

        private final Pattern pattern;
        private final int hash;

        private Then(TypeVariable owner, Pattern pattern) {
            super(owner);
            if (pattern == null) throw new NullPointerException("Null pattern");
            this.pattern = pattern;
            this.hash = Objects.hash(Then.class, this.owner, this.pattern);
        }

        public static Then of(final TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.Then constraint) {
            return new Then(owner, constraint.pattern());
        }

        public Pattern pattern() {
            return pattern;
        }

        @Override
        public Set<TypeVariable> variables() {
            return set();
        }

        @Override
        public boolean isThen() {
            return true;
        }

        @Override
        public TypeConstraint.Then asThen() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Then that = (Then) o;
            return (this.owner.equals(that.owner) && this.pattern.equals(that.pattern));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    // TODO: Move this out of TypeConstraint and create its own class
    public static class When extends TypeConstraint {

        private final Pattern pattern;
        private final int hash;

        private When(TypeVariable owner, Pattern pattern) {
            super(owner);
            if (pattern == null) throw new NullPointerException("Null Pattern");
            this.pattern = pattern;
            this.hash = Objects.hash(When.class, this.owner, this.pattern);
        }

        public static When of(final TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.When constraint) {
            return new When(owner, constraint.pattern());
        }

        public Pattern pattern() {
            return pattern;
        }

        @Override
        public Set<TypeVariable> variables() {
            return set();
        }


        @Override
        public boolean isWhen() {
            return true;
        }

        @Override
        public TypeConstraint.When asWhen() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            When that = (When) o;
            return (this.owner.equals(that.owner) && this.pattern.equals(that.pattern));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Owns extends TypeConstraint {

        private final TypeVariable attributeType;
        private final TypeVariable overriddenAttributeType;
        private final boolean isKey;
        private final int hash;

        private Owns(TypeVariable owner, TypeVariable attributeType,
                     @Nullable TypeVariable overriddenAttributeType, boolean isKey) {
            super(owner);
            this.attributeType = attributeType;
            this.overriddenAttributeType = overriddenAttributeType;
            this.isKey = isKey;
            this.hash = Objects.hash(Owns.class, this.owner, this.attributeType, this.overriddenAttributeType, this.isKey);
        }

        public static Owns of(TypeVariable owner,
                              graql.lang.pattern.constraint.TypeConstraint.Owns constraint,
                              VariableRegistry registry) {
            TypeVariable attributeType = registry.register(constraint.attribute());
            TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
            return new Owns(owner, attributeType, overriddenType, constraint.isKey());
        }

        public TypeVariable attribute() {
            return attributeType;
        }

        public Optional<TypeVariable> overridden() {
            return Optional.ofNullable(overriddenAttributeType);
        }

        public boolean isKey() {
            return isKey;
        }

        @Override
        public Set<TypeVariable> variables() {
            return overriddenAttributeType == null
                    ? set(attributeType)
                    : set(attributeType, overriddenAttributeType);
        }

        @Override
        public boolean isOwns() {
            return true;
        }

        @Override
        public TypeConstraint.Owns asOwns() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Owns that = (Owns) o;
            return (this.owner.equals(that.owner) &&
                    this.attributeType.equals(that.attributeType) &&
                    Objects.equals(this.overriddenAttributeType, that.overriddenAttributeType) &&
                    this.isKey == that.isKey);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Plays extends TypeConstraint {

        private final TypeVariable relationType;
        private final TypeVariable roleType;
        private final TypeVariable overriddenRoleType;
        private final int hash;

        private Plays(TypeVariable owner, @Nullable TypeVariable relationType,
                      TypeVariable roleType, @Nullable TypeVariable overriddenRoleType) {
            super(owner);
            if (roleType == null) throw new NullPointerException("Null role");
            this.relationType = relationType;
            this.roleType = roleType;
            this.overriddenRoleType = overriddenRoleType;
            this.hash = Objects.hash(Plays.class, this.owner, this.relationType, this.roleType, this.overriddenRoleType);
        }

        public static Plays of(final TypeVariable owner,
                               graql.lang.pattern.constraint.TypeConstraint.Plays constraint,
                               VariableRegistry registry) {
            TypeVariable roleType = registry.register(constraint.role());
            TypeVariable relationType = constraint.relation().map(registry::register).orElse(null);
            TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
            return new Plays(owner, relationType, roleType, overriddenType);
        }

        public Optional<TypeVariable> relation() {
            return Optional.ofNullable(relationType);
        }

        public TypeVariable role() {
            return roleType;
        }

        public Optional<TypeVariable> overridden() {
            return Optional.ofNullable(overriddenRoleType);
        }

        @Override
        public Set<TypeVariable> variables() {
            Set<TypeVariable> variables = new HashSet<>();
            variables.add(roleType);
            if (relationType != null) variables.add(relationType);
            if (overriddenRoleType != null) variables.add(overriddenRoleType);
            return variables;
        }

        @Override
        public boolean isPlays() {
            return true;
        }

        @Override
        public TypeConstraint.Plays asPlays() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Plays that = (Plays) o;
            return (this.owner.equals(that.owner) &&
                    this.roleType.equals(that.roleType) &&
                    Objects.equals(this.relationType, that.relationType) &&
                    Objects.equals(this.overriddenRoleType, that.overriddenRoleType));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Relates extends TypeConstraint {

        private final TypeVariable roleType;
        private final TypeVariable overriddenRoleType;
        private final int hash;

        private Relates(TypeVariable owner, TypeVariable roleType, @Nullable TypeVariable overriddenRoleType) {
            super(owner);
            if (roleType == null) throw new NullPointerException("Null role");
            this.roleType = roleType;
            this.overriddenRoleType = overriddenRoleType;
            this.hash = Objects.hash(Relates.class, this.owner, this.roleType, this.overriddenRoleType);
        }

        public static Relates of(final TypeVariable owner,
                                 graql.lang.pattern.constraint.TypeConstraint.Relates constraint,
                                 VariableRegistry registry) {
            TypeVariable roleType = registry.register(constraint.role());
            TypeVariable overriddenRoleType = constraint.overridden().map(registry::register).orElse(null);
            return new Relates(owner, roleType, overriddenRoleType);
        }

        public TypeVariable role() {
            return roleType;
        }

        public Optional<TypeVariable> overridden() {
            return Optional.ofNullable(overriddenRoleType);
        }

        @Override
        public Set<TypeVariable> variables() {
            return overriddenRoleType == null ? set(roleType) : set(roleType, overriddenRoleType);
        }

        @Override
        public boolean isRelates() {
            return true;
        }

        @Override
        public TypeConstraint.Relates asRelates() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Relates that = (Relates) o;
            return (this.owner.equals(that.owner) &&
                    this.roleType.equals(that.roleType) &&
                    Objects.equals(this.overriddenRoleType, that.overriddenRoleType));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

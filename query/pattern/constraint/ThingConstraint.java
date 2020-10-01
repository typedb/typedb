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

import grakn.common.collection.Bytes;
import grakn.core.common.exception.GraknException;
import grakn.core.query.pattern.variable.ThingVariable;
import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.Variable;
import grakn.core.query.pattern.variable.VariableRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Query.INVALID_CASTING;
import static java.util.stream.Collectors.toList;

public class ThingConstraint extends Constraint {

    final ThingVariable owner;

    private ThingConstraint(ThingVariable owner) {
        this.owner = owner;
    }

    public static ThingConstraint of(final ThingVariable owner,
                                     final graql.lang.pattern.constraint.ThingConstraint constraint,
                                     final VariableRegistry register) {
        if (constraint.isIID()) return ThingConstraint.IID.of(owner, constraint.asIID());
        else if (constraint.isIsa()) return ThingConstraint.Isa.of(owner, constraint.asIsa(), register);
        else if (constraint.isNEQ()) return ThingConstraint.NEQ.of(owner, constraint.asNEQ(), register);
        else if (constraint.isValue()) return ThingConstraint.Value.of(owner, constraint.asValue(), register);
        else if (constraint.isRelation()) return ThingConstraint.Relation.of(owner, constraint.asRelation(), register);
        else if (constraint.isHas()) return ThingConstraint.Has.of(owner, constraint.asHas(), register);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    public ThingVariable owner() {
        return owner;
    }

    @Override
    public Set<Variable> variables() {
        return set();
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

    public boolean isNEQ() {
        return false;
    }

    public boolean isValue() {
        return false;
    }

    public boolean isRelation() {
        return false;
    }

    public boolean isHas() {
        return false;
    }

    public IID asIID() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(IID.class)));
    }

    public ThingConstraint.Isa asIsa() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Isa.class)));
    }

    public ThingConstraint.NEQ asNEQ() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(NEQ.class)));
    }

    public ThingConstraint.Value<?> asValue() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Value.class)));
    }

    public ThingConstraint.Relation asRelation() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Relation.class)));
    }

    public ThingConstraint.Has asHas() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Has.class)));
    }

    public static class IID extends ThingConstraint {

        private final byte[] iid;
        private final int hash;

        public IID(ThingVariable owner, byte[] iid) {
            super(owner);
            this.iid = iid;
            this.hash = Objects.hash(IID.class, this.owner, Arrays.hashCode(this.iid));
        }

        public static IID of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.IID constraint) {
            return new IID(owner, Bytes.hexStringToBytes(constraint.iid()));
        }

        public byte[] iid() {
            return iid;
        }

        @Override
        public boolean isIID() {
            return true;
        }

        @Override
        public IID asIID() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IID that = (IID) o;
            return (this.owner.equals(that.owner) && Arrays.equals(this.iid, that.iid));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Isa extends ThingConstraint {

        private final TypeVariable type;
        private final boolean isExplicit;
        private final int hash;

        private Isa(ThingVariable owner, TypeVariable type, boolean isExplicit) {
            super(owner);
            this.type = type;
            this.isExplicit = isExplicit;
            this.hash = Objects.hash(Isa.class, this.owner, this.type, this.isExplicit);
        }

        public static Isa of(ThingVariable owner,
                             graql.lang.pattern.constraint.ThingConstraint.Isa constraint,
                             VariableRegistry registry) {
            return new Isa(owner, registry.register(constraint.type()), constraint.isExplicit());
        }

        public TypeVariable type() {
            return type;
        }

        public boolean isExplicit() {
            return isExplicit;
        }

        @Override
        public Set<Variable> variables() {
            return set(type);
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public ThingConstraint.Isa asIsa() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Isa that = (Isa) o;
            return (this.owner.equals(that.owner) &&
                    this.type.equals(that.type) &&
                    this.isExplicit == that.isExplicit);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class NEQ extends ThingConstraint {

        private final ThingVariable variable;
        private final int hash;

        private NEQ(ThingVariable owner, ThingVariable variable) {
            super(owner);
            this.variable = variable;
            this.hash = Objects.hash(NEQ.class, this.owner, this.variable);
        }

        public static NEQ of(ThingVariable owner,
                             graql.lang.pattern.constraint.ThingConstraint.NEQ constraint,
                             VariableRegistry registry) {
            return new NEQ(owner, registry.register(constraint.variable()));
        }

        public ThingVariable variable() {
            return variable;
        }

        @Override
        public Set<Variable> variables() {
            return set(variable());
        }

        @Override
        public boolean isNEQ() {
            return true;
        }

        @Override
        public ThingConstraint.NEQ asNEQ() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NEQ that = (NEQ) o;
            return (this.owner.equals(that.owner) && this.variable.equals(that.variable));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Value<T> extends ThingConstraint {

        private final ValueOperation<T> operation;
        private final int hash;

        private Value(ThingVariable owner, ValueOperation<T> operation) {
            super(owner);
            this.operation = operation;
            this.hash = Objects.hash(Value.class, this.owner, this.operation);
        }

        public static Value<?> of(ThingVariable owner,
                                  graql.lang.pattern.constraint.ThingConstraint.Value<?> constraint,
                                  VariableRegistry registry) {
            return new Value<>(owner, ValueOperation.of(constraint.operation(), registry));
        }

        public ValueOperation<T> operation() {
            return operation;
        }

        @Override
        public Set<Variable> variables() {
            return operation.variable().isPresent() ? set(operation.variable().get()) : set();
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public ThingConstraint.Value<?> asValue() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Value<?> that = (Value<?>) o;
            return (this.owner.equals(that.owner) && this.operation.equals(that.operation));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Relation extends ThingConstraint {

        private final List<RolePlayer> players;
        private final int hash;

        private Relation(ThingVariable owner, List<RolePlayer> players) {
            super(owner);
            assert players != null && !players.isEmpty();
            this.players = new ArrayList<>(players);
            this.hash = Objects.hash(Relation.class, this.owner, this.players);
        }

        public static Relation of(ThingVariable owner,
                                  graql.lang.pattern.constraint.ThingConstraint.Relation constraint,
                                  VariableRegistry register) {
            return new Relation(owner, constraint.players().stream().map(rp -> RolePlayer.of(rp, register)).collect(toList()));
        }

        public List<RolePlayer> players() {
            return players;
        }

        @Override
        public Set<Variable> variables() {
            Set<Variable> variables = new HashSet<>();
            players().forEach(player -> {
                variables.add(player.player());
                if (player.roleType().isPresent()) variables.add(player.roleType().get());
            });
            return variables;
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public ThingConstraint.Relation asRelation() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Relation that = (Relation) o;
            return (this.owner.equals(that.owner) && this.players.equals(that.players));
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static class RolePlayer {

            private final TypeVariable roleType;
            private final ThingVariable player;
            private final int hash;

            private RolePlayer(@Nullable TypeVariable roleType, ThingVariable player) {
                if (player == null) throw new NullPointerException("Null player");
                this.roleType = roleType;
                this.player = player;
                this.hash = Objects.hash(this.roleType, this.player);
            }

            public static RolePlayer of(graql.lang.pattern.constraint.ThingConstraint.Relation.RolePlayer constraint,
                                        VariableRegistry registry) {
                return new RolePlayer(
                        constraint.roleType().map(registry::register).orElse(null),
                        registry.register(constraint.player())
                );
            }

            public Optional<TypeVariable> roleType() {
                return Optional.ofNullable(roleType);
            }

            public ThingVariable player() {
                return player;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                RolePlayer that = (RolePlayer) o;
                return (Objects.equals(this.roleType, that.roleType)) && (this.player.equals(that.player));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }

    public static class Has extends ThingConstraint {

        private final TypeVariable type;
        private final ThingVariable attribute;
        private final int hash;

        private Has(ThingVariable owner, TypeVariable type, ThingVariable attribute) {
            super(owner);
            assert type != null && attribute != null;
            this.type = type;
            this.attribute = attribute;
            this.hash = Objects.hash(Has.class, this.owner, this.type, this.attribute);
        }

        public static Has of(ThingVariable owner,
                             graql.lang.pattern.constraint.ThingConstraint.Has constraint,
                             VariableRegistry register) {
            return new Has(owner, register.register(constraint.type()), register.register(constraint.attribute()));
        }

        public TypeVariable type() {
            return type;
        }

        public ThingVariable attribute() {
            return attribute;
        }

        @Override
        public Set<Variable> variables() {
            return set(attribute);
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public ThingConstraint.Has asHas() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Has that = (Has) o;
            return (this.owner.equals(that.owner) &&
                    this.type.equals(that.type) &&
                    this.attribute.equals(that.attribute));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.query.pattern.property.VarProperty;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * A variable in a Graql query
 */
public class Var extends VarPattern {

    private static final Pattern VALID_VAR = Pattern.compile("[a-zA-Z0-9_-]+");

    private final String name;
    private final Type type;
    private volatile String label;

    public Var(String name, Type type) {
        if (name == null) {
            throw new NullPointerException("Null name");
        }
        Preconditions.checkArgument(
                VALID_VAR.matcher(name).matches(),
                "Var value [%s] is invalid. Must match regex %s", name, VALID_VAR
        );
        this.name = name;
        if (type == null) {
            throw new NullPointerException("Null kind");
        }
        this.type = type;
    }

    /**
     * Get the name of the variable (without prefixed "$")
     */
    public String name() {
        return name;
    }

    /**
     * The {@link Type} of the {@link Var}, such as whether it is user-defined.
     */
    public Type type() {
        return type;
    }

    /**
     * Whether the variable has been manually defined or automatically generated.
     *
     * @return whether the variable has been manually defined or automatically generated.
     */
    public boolean isUserDefinedName() {
        return type() == Type.UserDefined;
    }

    /**
     * Transform the variable into a user-defined one, retaining the generated label.
     * This is useful for "reifying" an existing variable.
     *
     * @return a new variable with the same label as the previous, but set as user-defined.
     */
    public Var asUserDefined() {
        if (isUserDefinedName()) {
            return this;
        } else {
            return new Var(name(), Type.UserDefined);
        }
    }

    /**
     * Get a unique label identifying the variable, differentiating user-defined variables from generated ones.
     */
    public String label() {
        if (label == null) {
            synchronized (this) {
                if (label == null) {
                    label = type().prefix() + name();
                }
            }
        }
        return label;
    }

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    public String shortName() {
        return type().prefix() + StringUtils.right(name(), 3);
    }

    @Override
    public Var var() {
        return this;
    }

    @Override
    protected Set<VarProperty> properties() {
        return ImmutableSet.of();
    }

    @Override
    public String toString() {
        return "$" + name();
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it ignores whether a variable is user-defined
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Var varName = (Var) o;

        return name().equals(varName.name());
    }

    @Override
    public final int hashCode() {
        // This hashCode implementation is special: it ignores whether a variable is user-defined
        return name().hashCode();
    }

    /**
     * The {@link Type} of a {@link Var}, such as whether it is user-defined.
     */
    public enum Type {

        UserDefined {
            @Override
            public char prefix() {
                return '§';
            }
        },

        Generated {
            @Override
            public char prefix() {
                return '#';
            }
        },

        Reserved {
            @Override
            public char prefix() {
                return '!';
            }
        };

        public abstract char prefix();
    }
}

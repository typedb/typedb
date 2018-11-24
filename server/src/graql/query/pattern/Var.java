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
public class Var extends AbstractVarPattern implements VarPattern {

    private static final Pattern VALID_VAR = Pattern.compile("[a-zA-Z0-9_-]+");

    private final String getValue;
    private final Kind kind;
    private volatile String name;

    public Var(String value, Kind kind) {
        if (value == null) {
            throw new NullPointerException("Null getValue");
        }
        Preconditions.checkArgument(
                VALID_VAR.matcher(value).matches(),
                "Var value [%s] is invalid. Must match regex %s", value, VALID_VAR
        );
        this.getValue = value;
        if (kind == null) {
            throw new NullPointerException("Null kind");
        }
        this.kind = kind;
    }

    /**
     * Get the string name of the variable (without prefixed "$")
     */
    public String getValue() {
        return getValue;
    }

    /**
     * The {@link Kind} of the {@link Var}, such as whether it is user-defined.
     */
    public Kind kind() {
        return kind;
    }

    /**
     * Whether the variable has been manually defined or automatically generated.
     *
     * @return whether the variable has been manually defined or automatically generated.
     */
    public boolean isUserDefinedName() {
        return kind() == Kind.UserDefined;
    }

    /**
     * Transform the variable into a user-defined one, retaining the generated name.
     * <p>
     * This is useful for "reifying" an existing variable.
     *
     * @return a new variable with the same name as the previous, but set as user-defined.
     */
    public Var asUserDefined() {
        if (isUserDefinedName()) {
            return this;
        } else {
            return new Var(getValue(), Kind.UserDefined);
        }
    }

    /**
     * Get a unique name identifying the variable, differentiating user-defined variables from generated ones.
     */
    public String name() {
        if (name == null) {
            synchronized (this) {
                if (name == null) {
                    name = kind().prefix() + getValue();
                }
            }
        }
        return name;
    }

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    public String shortName() {
        return kind().prefix() + StringUtils.right(getValue(), 3);
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
        return "$" + getValue();
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it ignores whether a variable is user-defined
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Var varName = (Var) o;

        return getValue().equals(varName.getValue());
    }

    @Override
    public final int hashCode() {
        // This hashCode implementation is special: it ignores whether a variable is user-defined
        return getValue().hashCode();
    }

    /**
     * The {@link Var.Kind} of a {@link Var}, such as whether it is user-defined.
     */
    public enum Kind {

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

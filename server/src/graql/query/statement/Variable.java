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

package grakn.core.graql.query.statement;

import graql.util.Token;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * A variable in a Graql query
 */
public class Variable {

    private static AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);
    private static final Pattern VALID_VAR = Pattern.compile("[a-zA-Z0-9_-]+");

    private final String name;
    private final Type type;
    private final boolean visible;
    private volatile String symbol;

    public Variable() {
        this(true);
    }

    public Variable(boolean visible) {
        this(Long.toString(counter.getAndIncrement()), Variable.Type.ANONYMOUS, visible);
    }

    public Variable(String name) {
        this(name, Variable.Type.NAMED);
    }

    public Variable(String name, Type type) {
        this(name, type, true);
    }

    private Variable(String name, Type type, boolean visible) {
        if (name == null) {
            throw new NullPointerException("Null name");
        } else if (!VALID_VAR.matcher(name).matches()) {
            throw new IllegalArgumentException("Variable name [" + name + "] is invalid. Must match regex " + VALID_VAR);
        }
        this.name = name;
        if (type == null) {
            throw new NullPointerException("Null kind");
        }
        this.type = type;
        this.visible = visible;
    }

    /**
     * Get the name of the variable (without prefixed "$")
     */
    public String name() {
        return name;
    }

    /**
     * The Type of the Variable, such as whether it is user-defined.
     */
    public Type type() {
        return type;
    }

    public boolean isVisible() {
        return visible;
    }

    /**
     * Whether the variable has been manually defined or automatically generated.
     *
     * @return whether the variable has been manually defined or automatically generated.
     */
    public boolean isUserDefinedName() {
        return type() == Type.NAMED;
    }

    /**
     * Transform the variable into a user-defined one, retaining the generated symbol.
     * This is useful for "reifying" an existing variable.
     *
     * @return a new variable with the same symbol as the previous, but set as user-defined.
     */
    public Variable asUserDefined() {
        if (isUserDefinedName()) {
            return this;
        } else {
            return new Variable(name(), Type.NAMED);
        }
    }

    /**
     * Get a unique symbol identifying the variable, differentiating user-defined variables from generated ones.
     */
    public String symbol() {
        if (symbol == null) {
            synchronized (this) {
                if (symbol == null) {
                    symbol = type().prefix() + name();
                }
            }
        }
        return symbol;
    }

    @Override
    public String toString() {
        if (isUserDefinedName()) {
            return Token.Char.$ + name();
        } else {
            return Token.Char.$_.toString();
        }
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it ignores whether a variable is user-defined
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Variable varName = (Variable) o;

        return name().equals(varName.name());
    }

    @Override
    public final int hashCode() {
        // This hashCode implementation is special: it ignores whether a variable is user-defined
        return name().hashCode();
    }

    /**
     * The Type of a Variable, such as whether it is user-defined.
     */
    public enum Type {

        NAMED {
            @Override
            public char prefix() {
                return '§';
            }
        },

        ANONYMOUS {
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

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

/**
 * A variable in a Graql query
 *
 * @author Felix Chapman
 */
public interface Var extends VarPattern {

    /**
     * Get the string name of the variable (without prefixed "$")
     */
    String getValue();

    /**
     * The {@link Kind} of the {@link Var}, such as whether it is user-defined.
     */
    Kind kind();

    /**
     * Whether the variable has been manually defined or automatically generated.
     * @return whether the variable has been manually defined or automatically generated.
     */
    boolean isUserDefinedName();

    /**
     * Transform the variable into a user-defined one, retaining the generated name.
     *
     * This is useful for "reifying" an existing variable.
     *
     * @return a new variable with the same name as the previous, but set as user-defined.
     */
    Var asUserDefined();

    /**
     * Get a unique name identifying the variable, differentiating user-defined variables from generated ones.
     */
    String name();

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    String shortName();

    String toString();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    /**
     * The {@link Kind} of a {@link Var}, such as whether it is user-defined.
     */
    enum Kind {

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

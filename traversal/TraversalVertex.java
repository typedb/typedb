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

package grakn.core.traversal;

import graql.lang.common.GraqlArg;

public class TraversalVertex {

    private final Identifier identifier;

    TraversalVertex(Identifier identifier) {
        this.identifier = identifier;
    }

    public Identifier identifier() {
        return identifier;
    }

    void type(String[] labels) {

    }

    void isAbstract() {

    }

    public void label(String label, String scope) {

    }

    public void regex(java.util.regex.Pattern regex) {

    }

    public void valueType(GraqlArg.ValueType valueType) {

    }

    public static class Pattern extends TraversalVertex {

        public Pattern(Identifier identifier, Traversal.Pattern pattern) {
            super(identifier);
        }
    }

    public static class Planner extends TraversalVertex {

        Planner(Identifier identifier, Traversal.Planner planner) {
            super(identifier);
        }
    }

    public static class Plan extends TraversalVertex {

        Plan(Identifier identifier, Traversal.Plan plan) {
            super(identifier);
        }

    }
}

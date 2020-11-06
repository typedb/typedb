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

import java.util.regex.Pattern;

public class TraversalVertex {

    private final Identifier identifier;
    private final TraversalPlan plan;

    TraversalVertex(Identifier identifier, TraversalPlan plan) {
        this.identifier = identifier;
        this.plan = plan;
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

    public void regex(Pattern regex) {

    }

    public void valueType(GraqlArg.ValueType valueType) {

    }
}

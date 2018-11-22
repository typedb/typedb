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

package grakn.core.graql.internal.template.macro;

import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.macro.Macro;

import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * <p>
 * Concatenate all of the given values to a string. Accepts at least two arguments.
 *
 * Usage:
 *      {@literal @}concat(<value1>, <value2>)
 * </p>
 */
public class ConcatMacro implements Macro<String> {

    @Override
    public String apply(List<Object> values) {
        if(values.size() < 2){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        return values.stream().map(Object::toString).collect(joining());
    }

    @Override
    public String name() {
        return "concat";
    }
}

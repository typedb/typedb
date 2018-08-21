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

package ai.grakn.graql.internal.template.macro;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.macro.Macro;

import java.util.List;

/**
 * <p>
 * Convert the given value into a quoted string. Only accepts one argument.
 *
 * Usage:
 *      {@literal @}string(<value>)
 * </p>
 * @author alexandraorth
 */
public class StringMacro implements Macro<String> {

    private static final int numberArguments = 1;

    @Override
    public String apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        return values.get(0).toString();
    }

    @Override
    public String name() {
        return "string";
    }
}

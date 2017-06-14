/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.template.macro;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.macro.Macro;

import java.util.List;

/**
 * <p>
 * Convert the given value into a long. Only accepts one argument.
 *
 * Usage:
 *      {@literal @}long(<value>)
 * </p>
 *
 * @author alexandraorth
 */
public class LongMacro implements Macro<Long> {

    private static final int numberArguments = 1;

    @Override
    public Long apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        String longValue = values.get(0).toString();
        try {
            return Long.parseLong(longValue);
        } catch (NumberFormatException e){
            throw GraqlQueryException.wrongMacroArgumentType(this, "a long", longValue);
        }
    }

    @Override
    public String name() {
        return "long";
    }
}

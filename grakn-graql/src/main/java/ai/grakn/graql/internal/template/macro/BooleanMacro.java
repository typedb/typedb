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

import ai.grakn.graql.macro.Macro;

import java.util.List;

/**
 * <p>
 * Templating function that will convert the given value into a boolean (true/false). Only accepts one argument.
 *
 * Usage:
 *      {@literal @}boolean(<value>)
 * </p>
 * @author alexandraorth
 */
public class BooleanMacro implements Macro<Boolean> {

    private static final int numberArguments = 1;

    @Override
    public Boolean apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        return Boolean.parseBoolean(values.get(0).toString());
    }

    @Override
    public String name() {
        return "boolean";
    }
}

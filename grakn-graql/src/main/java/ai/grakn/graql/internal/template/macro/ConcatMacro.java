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

import static java.util.stream.Collectors.joining;

/**
 * <p>
 * Concatenate all of the given values to a string. Accepts at least two arguments.
 *
 * Usage:
 *      {@literal @}concat(<value1>, <value2>)
 * </p>
 * @author alexandraorth
 */
public class ConcatMacro implements Macro<String> {

    @Override
    public String apply(List<Object> values) {
        if(values.size() < 2){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        return values.stream().map(this::toString).collect(joining());
    }

    @Override
    public String name() {
        return "concat";
    }

    public String toString(Object object){
        if(object instanceof UnescapedString){
            return ((UnescapedString) object).get();
        }
        return object.toString();
    }
}

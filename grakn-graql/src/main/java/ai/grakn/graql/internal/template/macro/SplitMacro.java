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

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * Templating function that splits the given value based on the given character. Accepts exactly two arguments.
 *
 * Usage:
 *      {@literal @}split(<value>, ",")
 *      {@literal @}split(<value>, "-")
 *      {@literal @}split(<value>, "and")
 * </p>
 *
 * @author alexandraorth
 */
public class SplitMacro implements Macro<List<String>> {

    private static final int numberArguments = 2;

    @Override
    public List<String> apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        String valueToSplit = values.get(0).toString();
        String splitString = values.get(1).toString();

        return Arrays.asList(valueToSplit.split(splitString));
    }

    @Override
    public String name() {
        return "split";
    }
}

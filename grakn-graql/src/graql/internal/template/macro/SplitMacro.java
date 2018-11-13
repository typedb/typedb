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

import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.macro.Macro;

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
 */
public class SplitMacro implements Macro<List<String>> {

    private static final int numberArguments = 2;

    @Override
    public List<String> apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
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

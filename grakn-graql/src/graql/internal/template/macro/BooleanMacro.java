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

import grakn.core.exception.GraqlQueryException;
import grakn.core.graql.macro.Macro;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * <p>
 * Templating function that will convert the given value into a boolean (true/false). Only accepts one argument.
 *
 * Usage:
 *      {@literal @}boolean(<value>)
 * </p>
 */
public class BooleanMacro implements Macro<Boolean> {

    private static final Collection<String> allowedBooleanValues = ImmutableSet.of("true", "false");
    private static final int numberArguments = 1;

    @Override
    public Boolean apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        String booleanValue = values.get(0).toString().toLowerCase(Locale.getDefault());
        if(!allowedBooleanValues.contains(booleanValue)){
            throw GraqlQueryException.wrongMacroArgumentType(this, booleanValue);
        }

        return Boolean.parseBoolean(booleanValue);
    }

    @Override
    public String name() {
        return "boolean";
    }
}

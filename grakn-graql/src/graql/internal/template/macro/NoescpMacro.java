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

import java.util.List;

/**
 * <p>
 * Convert the given value into an unescaped. Only accepts one argument.
 *
 * Usage:
 *      {@literal @}noescp(<value>)
 * </p>
 *
 */
public class NoescpMacro implements Macro<Unescaped> {

    private static final int numberArguments = 1;

    @Override
    public Unescaped apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        return Unescaped.of(values.get(0).toString());
    }

    @Override
    public String name(){
        return "noescp";
    }
}

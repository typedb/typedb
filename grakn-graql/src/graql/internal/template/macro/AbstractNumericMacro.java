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
 *     {@link Number} parsing {@link Macro}
 * </p>
 *
 * <p>
 *     Represents the base {@link Macro} exclusively used for {@link Number}s, such as {@link LongMacro} and
 *     {@link IntMacro}
 * </p>
 *
 * @param <T> A number which inherits from the {@link Number} class
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class AbstractNumericMacro<T extends Number> implements Macro<Number>{
    private static final int numberArguments = 1;

    @Override
    public Number apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        String value = values.get(0).toString();
        try {
            return convertNumeric(value);
        }  catch (NumberFormatException e){
            throw GraqlQueryException.wrongMacroArgumentType(this, value);
        }
    }

    abstract T convertNumeric(String value);


}

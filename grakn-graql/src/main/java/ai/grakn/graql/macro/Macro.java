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

package ai.grakn.graql.macro;

import javax.annotation.CheckReturnValue;
import java.util.List;

/**
 * A macro function to perform on a template.
 *
 * @param <T> the type of result after applying this macro
 *
 * @author Alexandra Orth
 */
public interface Macro<T> {

    /**
     * Apply the macro to the given values
     * @param values Values on which to operate the macro
     * @return result of the function
     */
    @CheckReturnValue
    T apply(List<Object> values);

    @CheckReturnValue
    String name();
}

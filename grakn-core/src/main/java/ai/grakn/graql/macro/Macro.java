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

package ai.grakn.graql.macro;

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
    T apply(List<Object> values);

    /**
     * The name of the macro
     * @return the name of the macro
     */
    String name();
}

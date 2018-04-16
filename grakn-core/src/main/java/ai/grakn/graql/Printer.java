/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import javax.annotation.CheckReturnValue;

/**
 * Interface describing a way to print Graql objects.
 *
 * For example, if you wanted something that could convert Graql into a YAML representation, you might make a
 * {@code YamlPrinter implements Printer<Yaml>}, that would convert everything into a {@code Yaml} and define a method
 * to convert a {@code Yaml} into a {@code String}.
 *
 * @param <T> The type of the intermediate representation that can be converted into a string
 *
 * @author Felix Chapman
 */
public interface Printer<T> extends GraqlConverter<T, String> {

    /**
     * Convert any object into a string
     * @param object the object to convert to a string
     * @return the object as a string
     *
     * @deprecated use {@link #convert(Object)}
     */
    @Deprecated
    @CheckReturnValue
    default String graqlString(Object object) {
        return convert(object);
    }
}

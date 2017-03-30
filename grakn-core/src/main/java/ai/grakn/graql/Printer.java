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

package ai.grakn.graql;

import ai.grakn.concept.Concept;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

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
public interface Printer<T> {

    /**
     * Convert any object into a string
     * @param object the object to convert to a string
     * @return the object as a string
     */
    default String graqlString(Object object) {
        T builder = graqlString(false, object);
        return build(builder);
    }

    /**
     * Convert any object into a builder
     * @param inner whether this object is within a collection
     * @param object the object to convert into a builder
     * @return the object as a builder
     */
    default T graqlString(boolean inner, Object object) {
        if (object instanceof Concept) {
            return graqlString(inner, (Concept) object);
        } else if (object instanceof VarName) {
            return graqlString(inner, (VarName) object);
        } else if (object instanceof Boolean) {
            return graqlString(inner, (boolean) object);
        } else if (object instanceof Optional) {
            return graqlString(inner, (Optional<?>) object);
        } else if (object instanceof Collection) {
            return graqlString(inner, (Collection<?>) object);
        } else if (object instanceof Map) {
            return graqlString(inner, (Map<?, ?>) object);
        } else {
            return graqlStringDefault(inner, object);
        }
    }

    /**
     * Convert a builder into a string
     * @param builder the builder to convert into a string
     * @return the builder as a string
     */
    String build(T builder);

    /**
     * Convert any concept into a builder
     * @param inner whether this concept is within a collection
     * @param concept the concept to convert into a builder
     * @return the concept as a builder
     */
    T graqlString(boolean inner, Concept concept);

    /**
     * Convert any boolean into a builder
     * @param inner whether this boolean is within a collection
     * @param bool the boolean to convert into a builder
     * @return the boolean as a builder
     */
    T graqlString(boolean inner, boolean bool);

    /**
     * Convert any optional into a builder
     * @param inner whether this optional is within a collection
     * @param optional the optional to convert into a builder
     * @return the optional as a builder
     */
    T graqlString(boolean inner, Optional<?> optional);

    /**
     * Convert any collection into a builder
     * @param inner whether this collection is within a collection
     * @param collection the collection to convert into a builder
     * @return the collection as a builder
     */
    T graqlString(boolean inner, Collection<?> collection);

    /**
     * Convert any map into a builder
     * @param inner whether this map is within a collection
     * @param map the map to convert into a builder
     * @return the map as a builder
     */
    T graqlString(boolean inner, Map<?, ?> map);

    /**
     * Default conversion behaviour if none of the more specific methods can be used
     * @param inner whether this object is within a collection
     * @param object the object to convert into a builder
     * @return the object as a builder
     */
    T graqlStringDefault(boolean inner, Object object);
}

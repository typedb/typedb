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
 */
public interface Printer<T> {

    default String graqlString(Object object) {
        T builder = graqlString(false, object);
        return build(builder);
    }

    default T graqlString(boolean inner, Object object) {
        if (object instanceof Concept) {
            return graqlString(inner, (Concept) object);
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

    String build(T builder);

    T graqlString(boolean inner, Concept concept);

    T graqlString(boolean inner, boolean bool);

    T graqlString(boolean inner, Optional<?> optional);

    T graqlString(boolean inner, Collection<?> collection);

    T graqlString(boolean inner, Map<?, ?> map);

    T graqlStringDefault(boolean inner, Object object);
}

/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.core.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttributeUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AttributeUtil.class);

    public static boolean isWholeNumber(Number n) {
        return isWholeNumber(n.getClass());
    }

    public static boolean isWholeNumber(Class<?> clazz) {
        return clazz.equals(Long.class) || clazz.equals(Integer.class) ||
                clazz.equals(Short.class) || clazz.equals(Byte.class);
    }

    public static boolean isDecimal(Class<?> clazz) {
        return clazz.equals(Double.class) || clazz.equals(Float.class);
    }

    public static boolean isString(Object o) {
        return isString(o.getClass());
    }

    public static boolean isString(Class<?> clazz) {
        return clazz.equals(String.class);
    }

    /**
     * Compares the two elements like java.util.Comparator#compare(Object, Object) but returns
     * null in case the two elements are not comparable.
     */
    public static Integer compare(Object a, Object b) {
        if (a == b) return 0;
        if (a == null || b == null) return null;
        if (a instanceof Number && b instanceof Number) {
            Number an = (Number) a;
            Number bn = (Number) b;
            if (Double.isNaN(an.doubleValue()) || Double.isNaN(bn.doubleValue())) {
                if (Double.isNaN(an.doubleValue()) && Double.isNaN(bn.doubleValue())) return 0;
                else return null;
            } else {
                if (an.doubleValue() == bn.doubleValue()) {
                    return Long.compare(an.longValue(), bn.longValue());
                } else {
                    return Double.compare(an.doubleValue(), bn.doubleValue());
                }
            }
        } else {
            try {
                return ((Comparable) a).compareTo(b);
            } catch (Throwable e) {
                LOG.debug("Could not compare elements: {} - {}", a, b);
                return null;
            }
        }
    }

    public static boolean hasGenericDataType(PropertyKey key) {
        return key.dataType().equals(Object.class);
    }
}

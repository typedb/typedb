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

package grakn.core.graph.core.attribute;

import com.google.common.base.Preconditions;
import grakn.core.graph.graphdb.database.serialize.AttributeUtil;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import org.apache.commons.lang.ArrayUtils;

/**
 * Basic comparison relations for comparable (i.e. linearly ordered) objects.
 *
 */

public enum Cmp implements JanusGraphPredicate {

    EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean test(Object value, Object condition) {
            if (condition==null) {
                return value==null;
            } else {
                return condition.equals(value) || (condition.getClass().isArray() && ArrayUtils.isEquals(condition, value));
            }
        }

        @Override
        public String toString() {
            return "=";
        }

        @Override
        public JanusGraphPredicate negate() {
            return NOT_EQUAL;
        }
    },

    NOT_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean test(Object value, Object condition) {
            if (condition==null) {
                return value!=null;
            } else {
                return !condition.equals(value);
            }
        }

        @Override
        public String toString() {
            return "<>";
        }

        @Override
        public JanusGraphPredicate negate() {
            return EQUAL;
        }
    },

    LESS_THAN {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof Comparable;
        }

        @Override
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp < 0;
        }

        @Override
        public String toString() {
            return "<";
        }

        @Override
        public JanusGraphPredicate negate() {
            return GREATER_THAN_EQUAL;
        }
    },

    LESS_THAN_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof Comparable;
        }

        @Override
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp <= 0;
        }

        @Override
        public String toString() {
            return "<=";
        }

        @Override
        public JanusGraphPredicate negate() {
            return GREATER_THAN;
        }
    },

    GREATER_THAN {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof Comparable;
        }

        @Override
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp > 0;
        }

        @Override
        public String toString() {
            return ">";
        }

        @Override
        public JanusGraphPredicate negate() {
            return LESS_THAN_EQUAL;
        }
    },

    GREATER_THAN_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof Comparable;
        }

        @Override
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp >= 0;
        }

        @Override
        public String toString() {
            return ">=";
        }

        @Override
        public JanusGraphPredicate negate() {
            return LESS_THAN;
        }
    };

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public boolean isQNF() {
        return true;
    }

}

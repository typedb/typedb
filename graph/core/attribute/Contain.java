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
import grakn.core.graph.graphdb.query.JanusGraphPredicate;

import java.util.Collection;

/**
 * Comparison relations for text objects.
 */

public enum Contain implements JanusGraphPredicate {

    /**
     * Whether an element is in a collection
     */
    IN {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return col.contains(value);
        }

        @Override
        public JanusGraphPredicate negate() {
            return NOT_IN;
        }
    },

    /**
     * Whether an element is not in a collection
     */
    NOT_IN {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return !col.contains(value);
        }

        @Override
        public JanusGraphPredicate negate() {
            return IN;
        }

    };

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return true;
    }

    @Override
    public boolean isValidCondition(Object condition) {
        return (condition instanceof Collection);
    }

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public boolean isQNF() {
        return false;
    }


}

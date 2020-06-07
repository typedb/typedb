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

import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Text;


/**
 * A JanusGraphPredicate that just wraps a TinkerPop Text predicate.
 * <p>
 * This enables JanusGraph to use TinkerPop Text predicates in places where it expects a
 * JanusGraphPredicate.
 */
public class TinkerPopTextWrappingPredicate implements JanusGraphPredicate {
    private Text internalPredicate;

    public TinkerPopTextWrappingPredicate(Text wrappedPredicate) {
        internalPredicate = wrappedPredicate;
    }

    @Override
    public boolean isValidCondition(Object condition) {
        return condition instanceof String && StringUtils.isNoneBlank(condition.toString());
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return clazz.equals(String.class);
    }

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public JanusGraphPredicate negate() {
        return new TinkerPopTextWrappingPredicate(internalPredicate.negate());
    }

    @Override
    public boolean isQNF() {
        return true;
    }

    @Override
    public boolean test(Object value, Object condition) {
        return internalPredicate.test(value.toString(), (String) condition);
    }
}

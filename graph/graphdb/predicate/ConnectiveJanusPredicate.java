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

package grakn.core.graph.graphdb.predicate;

import grakn.core.graph.graphdb.query.JanusGraphPredicate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class ConnectiveJanusPredicate extends ArrayList<JanusGraphPredicate> implements JanusGraphPredicate {

    private static final long serialVersionUID = 1558788908114391360L;

    public ConnectiveJanusPredicate() {
        super();
    }

    public ConnectiveJanusPredicate(List<JanusGraphPredicate> predicates) {
        super(predicates);
    }

    abstract ConnectiveJanusPredicate getNewNegateIntance();

    abstract boolean isOr();

    @Override
    @SuppressWarnings("unchecked")
    public boolean isValidCondition(Object condition) {
        if (!(condition instanceof List) || ((List<?>) condition).size() != this.size()) {
            return false;
        }
        final Iterator<Object> itConditions = ((List<Object>) condition).iterator();
        return !this.stream().anyMatch(internalCondition -> !internalCondition.isValidCondition(itConditions.next()));
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return !this.stream().anyMatch(internalCondition -> !(internalCondition.isValidValueType(clazz)));
    }

    @Override
    public boolean hasNegation() {
        return !this.stream().anyMatch(internalCondition -> !(internalCondition.hasNegation()));
    }

    @Override
    public JanusGraphPredicate negate() {
        ConnectiveJanusPredicate toReturn = getNewNegateIntance();
        this.stream().map(JanusGraphPredicate::negate).forEach(toReturn::add);
        return toReturn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean test(Object value, Object condition) {
        if (!(condition instanceof List) || ((List<?>) condition).size() != this.size()) {
            return false;
        }
        Iterator<Object> itConditions = ((List<Object>) condition).iterator();
        return this.stream().anyMatch(internalCondition -> isOr() == internalCondition.test(value, itConditions.next())) == isOr();
    }
}

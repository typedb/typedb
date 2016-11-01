/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;
import ai.grakn.graql.internal.util.StringConverter;

abstract class ComparatorPredicate extends AbstractValuePredicate {

    protected Object value;

    /**
     * @param value the value that this atom is testing against
     */
    public ComparatorPredicate(Object value) {
        super(ImmutableSet.of(value));
        this.value = value;
    }

    protected abstract String getSymbol();

    public String toString() {
        return getSymbol() + " " + StringConverter.valueToString(value);
    }
}

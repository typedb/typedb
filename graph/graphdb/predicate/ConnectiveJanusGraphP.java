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
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectiveJanusGraphP extends P<Object> {

    private static final long serialVersionUID = 1737489543643777182L;

    public ConnectiveJanusGraphP(ConnectiveJanusPredicate biPredicate, List<Object> value) {
        super(biPredicate, value);
    }
    @Override
    public String toString() {
        return toString((ConnectiveJanusPredicate) this.biPredicate, this.originalValue).toString();
    }

    private StringBuilder toString(JanusGraphPredicate predicate, Object value) {
        final StringBuilder toReturn = new StringBuilder();
        if (!(predicate instanceof ConnectiveJanusPredicate)) {
            toReturn.append(predicate);
            if (value != null) {
                toReturn.append("(").append(value).append(")");
            }
            return toReturn;
        }
        final ConnectiveJanusPredicate connectivePredicate = (ConnectiveJanusPredicate) predicate;
        final List<Object> values = null == value ? new ArrayList<>() : (List<Object>) value;
        if (connectivePredicate.size() == 1) {
            return toString(connectivePredicate.get(0), values.get(0));
        }
        if (predicate instanceof AndJanusPredicate){
            toReturn.append("and(");
        } else if (predicate instanceof OrJanusPredicate){
            toReturn.append("or(");
        } else {
            throw new IllegalArgumentException("JanusGraph does not support the given predicate: " + predicate);
        }
        final Iterator<Object> itValues = values.iterator();
        toReturn.append(connectivePredicate.stream().map(p -> toString(p, itValues.next())).collect(Collectors.joining(", "))).append(")");
        return toReturn;
    }
}

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

package grakn.core.graph.graphdb.query;

import grakn.core.graph.core.attribute.Cmp;
import grakn.core.graph.core.attribute.Contain;
import grakn.core.graph.core.attribute.TinkerPopTextWrappingPredicate;
import grakn.core.graph.graphdb.predicate.AndJanusPredicate;
import grakn.core.graph.graphdb.predicate.ConnectiveJanusGraphP;
import grakn.core.graph.graphdb.predicate.ConnectiveJanusPredicate;
import grakn.core.graph.graphdb.predicate.OrJanusPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * A special kind of BiPredicate which marks all the predicates that are natively supported by
 * JanusGraph and known to the query optimizer. Contains some custom methods that JanusGraph needs for
 * query answering and evaluation.
 * <p>
 * This class contains a subclass used to convert Tinkerpop's BiPredicate implementations to the corresponding JanusGraph predicates.
 */
public interface JanusGraphPredicate extends BiPredicate<Object, Object> {

    /**
     * Whether the given condition is a valid condition for this predicate.
     * <p>
     * For instance, the Cmp#GREATER_THAN would require that the condition is comparable and not null.
     */
    boolean isValidCondition(Object condition);

    /**
     * Whether the given class is a valid data type for a value to which this predicate may be applied.
     * <p>
     * For instance, the Cmp#GREATER_THAN can only be applied to Comparable values.
     */
    boolean isValidValueType(Class<?> clazz);

    /**
     * Whether this predicate has a predicate that is semantically its negation.
     * For instance, Cmp#EQUAL and Cmp#NOT_EQUAL are negatives of each other.
     */
    boolean hasNegation();

    /**
     * Returns the negation of this predicate if it exists, otherwise an exception is thrown. Check #hasNegation() first.
     */
    @Override
    JanusGraphPredicate negate();

    /**
     * Returns true if this predicate is in query normal form.
     */
    boolean isQNF();


    @Override
    boolean test(Object value, Object condition);


    class Converter {

        /**
         * Convert Tinkerpop's comparison operators to JanusGraph's
         *
         * @param p Any predicate
         * @return A JanusGraphPredicate equivalent to the given predicate
         * @throws IllegalArgumentException if the given Predicate is unknown
         */
        public static JanusGraphPredicate convertInternal(BiPredicate p) {
            if (p instanceof JanusGraphPredicate) {
                return (JanusGraphPredicate) p;
            } else if (p instanceof Compare) {
                final Compare comp = (Compare) p;
                switch (comp) {
                    case eq:
                        return Cmp.EQUAL;
                    case neq:
                        return Cmp.NOT_EQUAL;
                    case gt:
                        return Cmp.GREATER_THAN;
                    case gte:
                        return Cmp.GREATER_THAN_EQUAL;
                    case lt:
                        return Cmp.LESS_THAN;
                    case lte:
                        return Cmp.LESS_THAN_EQUAL;
                    default:
                        throw new IllegalArgumentException("Unexpected comparator: " + comp);
                }
            } else if (p instanceof Contains) {
                final Contains con = (Contains) p;
                switch (con) {
                    case within:
                        return Contain.IN;
                    case without:
                        return Contain.NOT_IN;
                    default:
                        throw new IllegalArgumentException("Unexpected container: " + con);

                }
            } else return null;
        }

        public static JanusGraphPredicate convert(BiPredicate p) {
            JanusGraphPredicate janusgraphPredicate = convertInternal(p);
            if (janusgraphPredicate == null) {
                throw new IllegalArgumentException("JanusGraph does not support the given predicate: " + p);
            }
            return janusgraphPredicate;
        }

        public static boolean supports(BiPredicate p) {
            return convertInternal(p) != null;
        }

        public static HasContainer convert(HasContainer container) {
            if (!(container.getPredicate() instanceof ConnectiveP)) {
                return container;
            }
            final ConnectiveJanusPredicate connectivePredicate = instanceConnectiveJanusPredicate(container.getPredicate());
            return new HasContainer(container.getKey(), new ConnectiveJanusGraphP(connectivePredicate, convert(((ConnectiveP<?>) container.getPredicate()), connectivePredicate)));
        }

        public static ConnectiveJanusPredicate instanceConnectiveJanusPredicate(P<?> predicate) {
            final ConnectiveJanusPredicate connectivePredicate;
            if (predicate.getClass().isAssignableFrom(AndP.class)) {
                connectivePredicate = new AndJanusPredicate();
            } else if (predicate.getClass().isAssignableFrom(OrP.class)) {
                connectivePredicate = new OrJanusPredicate();
            } else {
                throw new IllegalArgumentException("JanusGraph does not support the given predicate: " + predicate);
            }
            return connectivePredicate;
        }

        public static List<Object> convert(ConnectiveP<?> predicate, ConnectiveJanusPredicate connectivePredicate) {
            final List<Object> toReturn = new ArrayList<>();
            for (P<?> p : predicate.getPredicates()) {
                if (p instanceof ConnectiveP) {
                    final ConnectiveJanusPredicate subPredicate = instanceConnectiveJanusPredicate(p);
                    toReturn.add(convert((ConnectiveP<?>) p, subPredicate));
                    connectivePredicate.add(subPredicate);
                } else if (p.getBiPredicate() instanceof Text) {
                    Text text = (Text) p.getBiPredicate();
                    connectivePredicate.add(new TinkerPopTextWrappingPredicate(text));
                    toReturn.add(p.getValue());
                } else {
                    connectivePredicate.add(Converter.convert(p.getBiPredicate()));
                    toReturn.add(p.getValue());
                }
            }
            return toReturn;
        }
    }
}

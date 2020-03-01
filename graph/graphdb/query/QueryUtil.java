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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.attribute.Cmp;
import grakn.core.graph.core.attribute.Contain;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.predicate.AndJanusPredicate;
import grakn.core.graph.graphdb.predicate.OrJanusPredicate;
import grakn.core.graph.graphdb.query.condition.And;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.query.condition.MultiCondition;
import grakn.core.graph.graphdb.query.condition.Not;
import grakn.core.graph.graphdb.query.condition.Or;
import grakn.core.graph.graphdb.query.condition.PredicateCondition;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods used in query optimization and processing.
 */
public class QueryUtil {

    public static int adjustLimitForTxModifications(StandardJanusGraphTx tx, int uncoveredAndConditions, int limit) {
        if (uncoveredAndConditions > 0) {
            int maxMultiplier = Integer.MAX_VALUE / limit;
            limit = limit * Math.min(maxMultiplier, (int) Math.pow(2, uncoveredAndConditions)); //(limit*3)/2+1;
        }

        if (tx.hasModifications()) {
            limit += Math.min(Integer.MAX_VALUE - limit, 5);
        }

        return limit;
    }

    public static int convertLimit(long limit) {
        if (limit >= Integer.MAX_VALUE) return Integer.MAX_VALUE;
        else return (int) limit;
    }

    public static int mergeLowLimits(int limit1, int limit2) {
        return Math.max(limit1, limit2);
    }

    public static int mergeHighLimits(int limit1, int limit2) {
        return Math.min(limit1, limit2);
    }

    public static InternalRelationType getType(StandardJanusGraphTx tx, String typeName) {
        RelationType t = tx.getRelationType(typeName);
        if (t == null && !tx.getConfiguration().getAutoSchemaMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalRelationType) t;
    }

    public static Iterable<JanusGraphVertex> getVertices(StandardJanusGraphTx tx, PropertyKey key, Object equalityCondition) {
        return tx.query().has(key, Cmp.EQUAL, equalityCondition).vertices();
    }

    public static Iterable<JanusGraphVertex> getVertices(StandardJanusGraphTx tx, String key, Object equalityCondition) {
        return tx.query().has(key, Cmp.EQUAL, equalityCondition).vertices();
    }

    public static Iterable<JanusGraphEdge> getEdges(StandardJanusGraphTx tx, PropertyKey key, Object equalityCondition) {
        return tx.query().has(key, Cmp.EQUAL, equalityCondition).edges();
    }

    public static Iterable<JanusGraphEdge> getEdges(StandardJanusGraphTx tx, String key, Object equalityCondition) {
        return tx.query().has(key, Cmp.EQUAL, equalityCondition).edges();
    }

    /**
     * Query-normal-form (QNF) for JanusGraph is a variant of CNF (conjunctive normal form) with negation inlined where possible
     */
    public static boolean isQueryNormalForm(Condition<?> condition) {
        if (isQNFLiteralOrNot(condition)) {
            return true;
        }
        if (!(condition instanceof And)) {
            return false;
        }
        for (Condition<?> child : ((And<?>) condition).getChildren()) {
            if (!isQNFLiteralOrNot(child)) {
                if (child instanceof Or) {
                    for (Condition<?> child2 : ((Or<?>) child).getChildren()) {
                        if (!isQNFLiteralOrNot(child2)) return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isQNFLiteralOrNot(Condition<?> condition) {
        if (!(condition instanceof Not)) {
            return isQNFLiteral(condition);
        }
        Condition child = ((Not) condition).getChild();
        return isQNFLiteral(child) && (!(child instanceof PredicateCondition) || !((PredicateCondition) child).getPredicate().hasNegation());
    }

    private static boolean isQNFLiteral(Condition<?> condition) {
        return condition.getType() == Condition.Type.LITERAL && (!(condition instanceof PredicateCondition) || ((PredicateCondition) condition).getPredicate().isQNF());
    }

    public static <E extends JanusGraphElement> Condition<E> simplifyQNF(Condition<E> condition) {
        Preconditions.checkArgument(isQueryNormalForm(condition));
        if (condition.numChildren() == 1) {
            Condition<E> child = ((And<E>) condition).get(0);
            if (child.getType() == Condition.Type.LITERAL) return child;
        }
        return condition;
    }

    public static boolean isEmpty(Condition<?> condition) {
        return condition.getType() != Condition.Type.LITERAL && condition.numChildren() == 0;
    }

    /**
     * Prepares the constraints from the query builder into a QNF compliant condition.
     * If the condition is invalid or trivially false, it returns null.
     *
     * see #isQueryNormalForm(Condition)
     */
    public static <E extends JanusGraphElement> And<E> constraints2QNF(StandardJanusGraphTx tx, List<PredicateCondition<String, E>> constraints) {
        And<E> conditions = new And<>(constraints.size() + 4);
        for (PredicateCondition<String, E> atom : constraints) {
            RelationType type = getType(tx, atom.getKey());
            JanusGraphPredicate predicate = atom.getPredicate();

            if (type == null) {
                if (predicate == Cmp.EQUAL && atom.getValue() == null || (predicate == Cmp.NOT_EQUAL && atom.getValue() != null)) {
                    continue; //Ignore condition, its trivially satisfied
                }
                return null;
            }

            Object value = atom.getValue();


            // TODO: verify if following checkArguments are needed
//            if (type.isPropertyKey()) {
//                PropertyKey key = (PropertyKey) type;
//                Preconditions.checkArgument(key.dataType() == Object.class || predicate.isValidValueType(key.dataType()), "Data type of key is not compatible with condition");
//            } else { //its a label
//                Preconditions.checkArgument(((EdgeLabel) type).isUnidirected());
//                Preconditions.checkArgument(predicate.isValidValueType(JanusGraphVertex.class), "Data type of key is not compatible with condition");
//            }

            if (predicate instanceof Contain) {
                //Rewrite contains conditions
                Collection values = (Collection) value;
                if (predicate == Contain.NOT_IN) {
                    if (values.isEmpty()) continue; //Simply ignore since trivially satisfied
                    for (Object inValue : values) {
                        addConstraint(type, Cmp.NOT_EQUAL, inValue, conditions, tx);
                    }
                } else {
                    Preconditions.checkArgument(predicate == Contain.IN);
                    if (values.isEmpty()) {
                        return null; //Cannot be satisfied
                    }
                    if (values.size() == 1) {
                        addConstraint(type, Cmp.EQUAL, values.iterator().next(), conditions, tx);
                    } else {
                        Or<E> nested = new Or<>(values.size());
                        for (Object invalue : values) {
                            addConstraint(type, Cmp.EQUAL, invalue, nested, tx);
                        }
                        conditions.add(nested);
                    }
                }
            } else if (predicate instanceof AndJanusPredicate) {
                if (addConstraint(type, (AndJanusPredicate) (predicate), (List<Object>) (value), conditions, tx) == null) {
                    return null;
                }
            } else if (predicate instanceof OrJanusPredicate) {
                List<Object> values = (List<Object>) (value);
                Or<E> nested = addConstraint(type, (OrJanusPredicate) predicate, values, new Or<>(values.size()), tx);
                if (nested == null) {
                    return null;
                }
                conditions.add(nested);
            } else {
                addConstraint(type, predicate, value, conditions, tx);
            }
        }
        return conditions;
    }

    private static <E extends JanusGraphElement> And<E> addConstraint(RelationType type, AndJanusPredicate predicate, List<Object> values, And<E> and, StandardJanusGraphTx tx) {
        for (int i = 0; i < values.size(); i++) {
            JanusGraphPredicate janusGraphPredicate = predicate.get(i);
            if (janusGraphPredicate instanceof AndJanusPredicate) {
                if (addConstraint(type, (AndJanusPredicate) (janusGraphPredicate), (List<Object>) (values.get(i)), and, tx) == null) {
                    return null;
                }
            } else if (predicate.get(i) instanceof OrJanusPredicate) {
                List<Object> childValues = (List<Object>) (values.get(i));
                Or<E> nested = addConstraint(type, (OrJanusPredicate) (janusGraphPredicate), childValues, new Or<>(childValues.size()), tx);
                if (nested == null) {
                    return null;
                }
                and.add(nested);
            } else {
                addConstraint(type, janusGraphPredicate, values.get(i), and, tx);
            }
        }
        return and;
    }

    private static <E extends JanusGraphElement> Or<E> addConstraint(RelationType type, OrJanusPredicate predicate, List<Object> values, Or<E> or, StandardJanusGraphTx tx) {
        for (int i = 0; i < values.size(); i++) {
            JanusGraphPredicate janusGraphPredicate = predicate.get(i);
            if (janusGraphPredicate instanceof AndJanusPredicate) {
                List<Object> childValues = (List<Object>) (values.get(i));
                And<E> nested = addConstraint(type, (AndJanusPredicate) janusGraphPredicate, childValues, new And<>(childValues.size()), tx);
                if (nested == null) {
                    return null;
                }
                or.add(nested);
            } else if (janusGraphPredicate instanceof OrJanusPredicate) {
                if (addConstraint(type, (OrJanusPredicate) janusGraphPredicate, (List<Object>) (values.get(i)), or, tx) == null) {
                    return null;
                }
            } else {
                addConstraint(type, janusGraphPredicate, values.get(i), or, tx);
            }
        }
        return or;
    }

    private static <E extends JanusGraphElement> void addConstraint(RelationType type, JanusGraphPredicate predicate,
                                                                    Object value, MultiCondition<E> conditions, StandardJanusGraphTx tx) {
        if (type.isPropertyKey()) {
            if (value != null) {
                value = tx.verifyAttribute((PropertyKey) type, value);
            }
        } else { //t.isEdgeLabel()
            Preconditions.checkArgument(value instanceof JanusGraphVertex);
        }
        PredicateCondition<RelationType, E> pc = new PredicateCondition<>(type, predicate, value);
        if (!conditions.contains(pc)) conditions.add(pc);
    }


    public static Map.Entry<RelationType, Collection> extractOrCondition(Or<JanusGraphRelation> condition) {
        RelationType masterType = null;
        List<Object> values = new ArrayList<>();
        for (Condition c : condition.getChildren()) {
            if (!(c instanceof PredicateCondition)) {
                return null;
            }
            PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition) c;
            if (atom.getPredicate() != Cmp.EQUAL) {
                return null;
            }
            Object value = atom.getValue();
            if (value == null) {
                return null;
            }
            RelationType type = atom.getKey();
            if (masterType == null) {
                masterType = type;
            } else if (!masterType.equals(type)) {
                return null;
            }
            values.add(value);
        }
        if (masterType == null) {
            return null;
        }
        return new AbstractMap.SimpleImmutableEntry(masterType, values);
    }


    public static <R> List<R> processIntersectingRetrievals(List<IndexCall<R>> retrievals, int limit) {
        Preconditions.checkArgument(!retrievals.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        List<R> results;
        /*
         * Iterate over the clauses in the and collection
         * query.getCondition().getChildren(), taking the intersection
         * of current results with cumulative results on each iteration.
         */
        //TODO: smarter limit estimation
        int multiplier = Math.min(16, (int) Math.pow(2, retrievals.size() - 1));
        int subLimit = Integer.MAX_VALUE;
        if (Integer.MAX_VALUE / multiplier >= limit) subLimit = limit * multiplier;
        boolean exhaustedResults;
        do {
            exhaustedResults = true;
            results = null;
            for (IndexCall<R> call : retrievals) {
                Collection<R> subResult;
                try {
                    subResult = call.call(subLimit);
                } catch (Exception e) {
                    throw new JanusGraphException("Could not process individual retrieval call ", e);
                }

                if (subResult.size() >= subLimit) exhaustedResults = false;
                if (results == null) {
                    results = Lists.newArrayList(subResult);
                } else {
                    Set<R> subResultSet = ImmutableSet.copyOf(subResult);
                    results.removeIf(o -> !subResultSet.contains(o));
                }
            }
            subLimit = (int) Math.min(Integer.MAX_VALUE - 1, Math.max(Math.pow(subLimit, 1.5), (subLimit + 1) * 2));
        } while (results != null && results.size() < limit && !exhaustedResults);
        return results;
    }


    public interface IndexCall<R> {

        Collection<R> call(int limit);
    }

}

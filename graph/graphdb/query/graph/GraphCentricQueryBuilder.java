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

package grakn.core.graph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphQuery;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.attribute.Cmp;
import grakn.core.graph.core.attribute.Contain;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.Query;
import grakn.core.graph.graphdb.query.QueryProcessor;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.query.condition.And;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.query.condition.ConditionUtil;
import grakn.core.graph.graphdb.query.condition.MultiCondition;
import grakn.core.graph.graphdb.query.condition.Or;
import grakn.core.graph.graphdb.query.condition.PredicateCondition;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.ParameterIndexField;
import grakn.core.graph.graphdb.types.system.ImplicitKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Builds a JanusGraphQuery, optimizes the query and compiles the result into a GraphCentricQuery which
 * is then executed through a QueryProcessor.
 *
 */
public class GraphCentricQueryBuilder implements JanusGraphQuery<GraphCentricQueryBuilder> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphCentricQueryBuilder.class);
    private static final int DEFAULT_NO_LIMIT = 1000;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 100000;

    private static final double EQUAL_CONDITION_SCORE = 4;
    private static final double OTHER_CONDITION_SCORE = 1;
    private static final double ORDER_MATCH = 2;
    private static final double ALREADY_MATCHED_ADJUSTOR = 0.1;
    private static final double CARDINALITY_SINGE_SCORE = 1000;
    private static final double CARDINALITY_OTHER_SCORE = 1000;


    /**
     * Transaction in which this query is executed.
     */
    private final StandardJanusGraphTx tx;
    /**
     * Serializer used to serialize the query conditions into backend queries.
     */
    private final IndexSerializer serializer;
    /**
     * The constraints added to this query. None by default.
     */
    private final List<PredicateCondition<String, JanusGraphElement>> constraints = new ArrayList<>(5);

    /**
     * List of constraints added to an Or query. None by default
     */
    private final List<List<PredicateCondition<String, JanusGraphElement>>> globalConstraints = new ArrayList<>();
    /**
     * The order in which the elements should be returned. None by default.
     */
    private OrderList orders = new OrderList();
    /**
     * The limit of this query. No limit by default.
     */
    private int limit = Query.NO_LIMIT;
    /**
     * The profiler observing this query
     */
    private QueryProfiler profiler = QueryProfiler.NO_OP; // Not clear why there is a NO_OP profiler that cannot be changed here, surely it doesn't look very useful

    public GraphCentricQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;
    }

    /* ---------------------------------------------------------------
     * Query Execution
     * ---------------------------------------------------------------
     */

    @Override
    public Iterable<JanusGraphVertex> vertices() {
        return iterables(constructQuery(ElementCategory.VERTEX), JanusGraphVertex.class);
    }

    @Override
    public Iterable<JanusGraphEdge> edges() {
        return iterables(constructQuery(ElementCategory.EDGE), JanusGraphEdge.class);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> properties() {
        return iterables(constructQuery(ElementCategory.PROPERTY), JanusGraphVertexProperty.class);
    }

    public <E extends JanusGraphElement> Iterable<E> iterables(GraphCentricQuery query, Class<E> aClass) {
        return Iterables.filter(new QueryProcessor<>(query, tx.elementProcessor), aClass);
    }


    /* ---------------------------------------------------------------
     * Query Construction
     * ---------------------------------------------------------------
     */

    public List<PredicateCondition<String, JanusGraphElement>> getConstraints() {
        return constraints;
    }

    public GraphCentricQueryBuilder profiler(QueryProfiler profiler) {
        Preconditions.checkNotNull(profiler);
        this.profiler = profiler;
        return this;
    }

    @Override
    public GraphCentricQueryBuilder has(String key, JanusGraphPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        Preconditions.checkArgument(predicate.isValidCondition(condition), "Invalid condition: %s", condition);
        if (predicate.equals(Contain.NOT_IN)) {
            // when querying `has(key, without(value))`, the query must also satisfy `has(key)`
            has(key);
        }
        constraints.add(new PredicateCondition<>(key, predicate, condition));
        return this;
    }

    public GraphCentricQueryBuilder has(PropertyKey key, JanusGraphPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        return has(key.name(), predicate, condition);
    }

    @Override
    public GraphCentricQueryBuilder has(String key) {
        return has(key, Cmp.NOT_EQUAL, null);
    }

    @Override
    public GraphCentricQueryBuilder hasNot(String key) {
        return has(key, Cmp.EQUAL, null);
    }

    @Override
    public GraphCentricQueryBuilder has(String key, Object value) {
        return has(key, Cmp.EQUAL, value);
    }

    @Override
    public GraphCentricQueryBuilder hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public <T extends Comparable<?>> GraphCentricQueryBuilder interval(String s, T t1, T t2) {
        has(s, Cmp.GREATER_THAN_EQUAL, t1);
        return has(s, Cmp.LESS_THAN, t2);
    }

    @Override
    public GraphCentricQueryBuilder limit(int limit) {
        Preconditions.checkArgument(limit >= 0, "Non-negative limit expected: %s", limit);
        this.limit = limit;
        return this;
    }

    @Override
    public GraphCentricQueryBuilder orderBy(String keyName, org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        Preconditions.checkArgument(tx.containsPropertyKey(keyName), "Provided key does not exist: %s", keyName);
        PropertyKey key = tx.getPropertyKey(keyName);
        Preconditions.checkArgument(key != null && order != null, "Need to specify and key and an order");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.dataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.name(), key.dataType());
        Preconditions.checkArgument(key.cardinality() == Cardinality.SINGLE,
                "Ordering is undefined on multi-valued key [%s]", key.name());
        Preconditions.checkArgument(!orders.containsKey(key));
        orders.add(key, Order.convert(order));
        return this;
    }

    @Override
    public GraphCentricQueryBuilder or(GraphCentricQueryBuilder subQuery) {
        this.globalConstraints.add(subQuery.getConstraints());
        return this;
    }

    public GraphCentricQuery constructQuery(ElementCategory resultType) {
        QueryProfiler optProfiler = profiler.addNested(QueryProfiler.OPTIMIZATION);
        optProfiler.startTimer();
        if (this.globalConstraints.isEmpty()) {
            this.globalConstraints.add(this.constraints);
        }
        GraphCentricQuery query = constructQueryWithoutProfile(resultType);
        optProfiler.stopTimer();
        query.observeWith(profiler);
        return query;
    }

    private GraphCentricQuery constructQueryWithoutProfile(ElementCategory resultType) {
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare constraints
        MultiCondition<JanusGraphElement> conditions;
        if (this.globalConstraints.size() == 1) {
            conditions = QueryUtil.constraints2QNF(tx, constraints);
            if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);
        } else {
            conditions = new Or<>();
            for (List<PredicateCondition<String, JanusGraphElement>> child : this.globalConstraints) {
                And<JanusGraphElement> localconditions = QueryUtil.constraints2QNF(tx, child);
                if (localconditions == null) return GraphCentricQuery.emptyQuery(resultType);
                conditions.add(localconditions);
            }
        }


        //Prepare orders
        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        //Compile all indexes that cover at least one of the query conditions
        Set<IndexType> indexCandidates = new HashSet<>();
        ConditionUtil.traversal(conditions, condition -> {
            if (condition instanceof PredicateCondition) {
                RelationType type = ((PredicateCondition<RelationType, JanusGraphElement>) condition).getKey();
                Preconditions.checkArgument(type != null && type.isPropertyKey());
                Iterables.addAll(indexCandidates, Iterables.filter(((InternalRelationType) type).getKeyIndexes(),
                        indexType -> indexType.getElement() == resultType && !(conditions instanceof Or && (indexType.isCompositeIndex() || !serializer.features((MixedIndexType) indexType).supportNotQueryNormalForm()))));
            }
            return true;
        });

        /*
        Determine the best join index query to answer this query:
        Iterate over all potential indexes (as compiled above) and compute a score based on how many clauses
        this index covers. The index with the highest score (as long as it covers at least one additional clause)
        is picked and added to the joint query for as long as such exist.
         */
        JointIndexQuery jointQuery = new JointIndexQuery();
        boolean isSorted = orders.isEmpty();
        Set<Condition> coveredClauses = Sets.newHashSet();
        while (true) {
            IndexType bestCandidate = null;
            double candidateScore = 0.0;
            Set<Condition> candidateSubcover = null;
            boolean candidateSupportsSort = false;
            Object candidateSubCondition = null;

            for (IndexType index : indexCandidates) {
                Set<Condition> subcover = Sets.newHashSet();
                Object subCondition;
                boolean supportsSort = orders.isEmpty();
                //Check that this index actually applies in case of a schema constraint
                if (index.hasSchemaTypeConstraint()) {
                    JanusGraphSchemaType type = index.getSchemaTypeConstraint();
                    Map.Entry<Condition, Collection<Object>> equalCon
                            = getEqualityConditionValues(conditions, ImplicitKey.LABEL);
                    if (equalCon == null) continue;
                    Collection<Object> labels = equalCon.getValue();
                    if (labels.size() > 1) {
                        LOG.warn("The query optimizer currently does not support multiple label constraints in query: {}", this);
                        continue;
                    }
                    if (!type.name().equals(Iterables.getOnlyElement(labels))) {
                        continue;
                    }
                    subcover.add(equalCon.getKey());
                }

                if (index.isCompositeIndex()) {
                    subCondition = indexCover((CompositeIndexType) index, conditions, subcover);
                } else {
                    subCondition = indexCover((MixedIndexType) index, conditions, serializer, subcover);
                    if (coveredClauses.isEmpty() && !supportsSort && indexCoversOrder((MixedIndexType) index, orders)) {
                        supportsSort = true;
                    }
                }
                if (subCondition == null || subcover.isEmpty()) continue;
                double score = 0.0;
                boolean coversAdditionalClause = false;
                for (Condition c : subcover) {
                    double s = (c instanceof PredicateCondition && ((PredicateCondition) c).getPredicate() == Cmp.EQUAL) ? EQUAL_CONDITION_SCORE : OTHER_CONDITION_SCORE;
                    if (coveredClauses.contains(c)) {
                        s = s * ALREADY_MATCHED_ADJUSTOR;
                    } else {
                        coversAdditionalClause = true;
                    }
                    score += s;
                    if (index.isCompositeIndex()) {
                        score += ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE ? CARDINALITY_SINGE_SCORE : CARDINALITY_OTHER_SCORE;
                    }
                }
                if (supportsSort) score += ORDER_MATCH;
                if (coversAdditionalClause && score > candidateScore) {
                    candidateScore = score;
                    bestCandidate = index;
                    candidateSubcover = subcover;
                    candidateSubCondition = subCondition;
                    candidateSupportsSort = supportsSort;
                }
            }
            if (bestCandidate != null) {
                if (coveredClauses.isEmpty()) isSorted = candidateSupportsSort;
                coveredClauses.addAll(candidateSubcover);
                if (bestCandidate.isCompositeIndex()) {
                    jointQuery.add((CompositeIndexType) bestCandidate, serializer.getQuery((CompositeIndexType) bestCandidate, (List<Object[]>) candidateSubCondition));
                } else {
                    jointQuery.add((MixedIndexType) bestCandidate, serializer.getQuery((MixedIndexType) bestCandidate, (Condition) candidateSubCondition, orders));
                }
            } else {
                break;
            }
            /* TODO: smarter optimization:
            - use in-memory histograms to estimate selectivity of PredicateConditions and filter out low-selectivity ones
                    if they would result in an individual index call (better to filter afterwards in memory)
            - move OR's up and extend GraphCentricQuery to allow multiple JointIndexQuery for proper or'ing of queries
            */
        }

        BackendQueryHolder<JointIndexQuery> query;
        if (!coveredClauses.isEmpty()) {
            int indexLimit = limit == Query.NO_LIMIT ? HARD_MAX_LIMIT : limit;
            if (tx.getGraph().getConfiguration().adjustQueryLimit()) {
                indexLimit = limit == Query.NO_LIMIT ? DEFAULT_NO_LIMIT : Math.min(MAX_BASE_LIMIT, limit);
            }
            indexLimit = Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, coveredClauses.size(), indexLimit));
            jointQuery.setLimit(indexLimit);
            query = new BackendQueryHolder<>(jointQuery, coveredClauses.size() == conditions.numChildren(), isSorted);
        } else {
            query = new BackendQueryHolder<>(new JointIndexQuery(), false, isSorted);
        }
        return new GraphCentricQuery(resultType, conditions, orders, query, limit);
    }

    public static boolean indexCoversOrder(MixedIndexType index, OrderList orders) {
        for (int i = 0; i < orders.size(); i++) {
            if (!index.indexesKey(orders.getKey(i))) return false;
        }
        return true;
    }

    private static List<Object[]> indexCover(CompositeIndexType index, Condition<JanusGraphElement> condition, Set<Condition> covered) {
        if (!QueryUtil.isQueryNormalForm(condition)) {
            return null;
        }
        if (index.getStatus() != SchemaStatus.ENABLED) {
            return null;
        }
        IndexField[] fields = index.getFieldKeys();
        Object[] indexValues = new Object[fields.length];
        Set<Condition> coveredClauses = new HashSet<>(fields.length);
        List<Object[]> indexCovers = new ArrayList<>(4);

        constructIndexCover(indexValues, 0, fields, condition, indexCovers, coveredClauses);
        if (!indexCovers.isEmpty()) {
            covered.addAll(coveredClauses);
            return indexCovers;
        } else {
            return null;
        }
    }

    private static void constructIndexCover(Object[] indexValues, int position, IndexField[] fields, Condition<JanusGraphElement> condition,
                                            List<Object[]> indexCovers, Set<Condition> coveredClauses) {
        if (position >= fields.length) {
            indexCovers.add(indexValues);
        } else {
            IndexField field = fields[position];
            Map.Entry<Condition, Collection<Object>> equalCon = getEqualityConditionValues(condition, field.getFieldKey());
            if (equalCon != null) {
                coveredClauses.add(equalCon.getKey());
                for (Object value : equalCon.getValue()) {
                    Object[] newValues = Arrays.copyOf(indexValues, fields.length);
                    newValues[position] = value;
                    constructIndexCover(newValues, position + 1, fields, condition, indexCovers, coveredClauses);
                }
            }
        }
    }

    private static Map.Entry<Condition, Collection<Object>> getEqualityConditionValues(
            Condition<JanusGraphElement> condition, RelationType type) {
        for (Condition c : condition.getChildren()) {
            if (c instanceof Or) {
                Map.Entry<RelationType, Collection> orEqual = QueryUtil.extractOrCondition((Or) c);
                if (orEqual != null && orEqual.getKey().equals(type) && !orEqual.getValue().isEmpty()) {
                    return new AbstractMap.SimpleImmutableEntry(c, orEqual.getValue());
                }
            } else if (c instanceof PredicateCondition) {
                PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition) c;
                if (atom.getKey().equals(type) && atom.getPredicate() == Cmp.EQUAL && atom.getValue() != null) {
                    return new AbstractMap.SimpleImmutableEntry(c, ImmutableList.of(atom.getValue()));
                }
            }

        }
        return null;
    }

    private static Condition<JanusGraphElement> indexCover(MixedIndexType index, Condition<JanusGraphElement> condition, IndexSerializer indexInfo, Set<Condition> covered) {
        if (!indexInfo.features(index).supportNotQueryNormalForm() && !QueryUtil.isQueryNormalForm(condition)) {
            return null;
        }
        if (condition instanceof Or) {
            for (Condition<JanusGraphElement> subClause : condition.getChildren()) {
                if (subClause instanceof And) {
                    for (Condition<JanusGraphElement> subsubClause : condition.getChildren()) {
                        if (!coversAll(index, subsubClause, indexInfo)) {
                            return null;
                        }
                    }
                } else {
                    if (!coversAll(index, subClause, indexInfo)) {
                        return null;
                    }
                }
            }
            covered.add(condition);
            return condition;
        }
        And<JanusGraphElement> subCondition = new And<>(condition.numChildren());
        for (Condition<JanusGraphElement> subClause : condition.getChildren()) {
            if (coversAll(index, subClause, indexInfo)) {
                subCondition.add(subClause);
                covered.add(subClause);
            }
        }
        return subCondition.isEmpty() ? null : subCondition;
    }

    private static boolean coversAll(MixedIndexType index, Condition<JanusGraphElement> condition, IndexSerializer indexInfo) {
        if (condition.getType() != Condition.Type.LITERAL) {
            return StreamSupport.stream(condition.getChildren().spliterator(), false)
                    .allMatch(child -> coversAll(index, child, indexInfo));
        }
        if (!(condition instanceof PredicateCondition)) {
            return false;
        }
        PredicateCondition<RelationType, JanusGraphElement> atom = (PredicateCondition) condition;
        if (atom.getValue() == null) {
            return false;
        }

        Preconditions.checkArgument(atom.getKey().isPropertyKey());
        PropertyKey key = (PropertyKey) atom.getKey();
        ParameterIndexField[] fields = index.getFieldKeys();
        ParameterIndexField match = Arrays.stream(fields)
                .filter(field -> field.getStatus() == SchemaStatus.ENABLED)
                .filter(field -> field.getFieldKey().equals(key))
                .findAny().orElse(null);
        return match != null && indexInfo.supports(index, match, atom.getPredicate());
    }


}

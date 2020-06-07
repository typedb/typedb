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
import com.google.common.collect.Lists;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphIndexQuery;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.util.StreamIterable;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Stream;

/**
 * Implementation of JanusGraphIndexQuery for string based queries that are issued directly against the specified
 * indexing backend. It is assumed that the given string conforms to the query language of the indexing backend.
 * This class does not understand or verify the provided query. However, it will introspect the query and replace
 * any reference to `v.SOME_KEY`, `e.SOME_KEY` or `p.SOME_KEY` with the respective key reference. This replacement
 * is 'dumb' in the sense that it relies on simple string replacements to accomplish this. If the key contains special characters
 * (in particular space) then it must be encapsulated in quotation marks.
 * <p>
 * In addition to the query string, a number of parameters can be specified which will be passed verbatim to the indexing
 * backend during query execution.
 * <p>
 * This class essentially just acts as a builder, uses the IndexSerializer to execute the query, and then post-processes
 * the result set to return to the user.
 */
public class IndexQueryBuilder extends BaseQuery implements JanusGraphIndexQuery {

    private static final Logger LOG = LoggerFactory.getLogger(IndexQueryBuilder.class);

    private static final String VERTEX_PREFIX = "v.";
    private static final String EDGE_PREFIX = "e.";
    private static final String PROPERTY_PREFIX = "p.";

    private final StandardJanusGraphTx tx;
    private final IndexSerializer serializer;

    /**
     * The name of the indexing backend this query is directed at
     */
    private final String indexName;
    /**
     * Query string conforming to the query language supported by the indexing backend.
     */
    private final String query;
    /**
     * Sorting parameters
     */
    private final List<Parameter<Order>> orders;
    /**
     * Parameters passed to the indexing backend during query execution to modify the execution behavior.
     */
    private final List<Parameter> parameters;

    /**
     * Prefix to be used to identify vertex, edge or property references and trigger key parsing and conversion.
     * In most cases this will be one of the above defined static prefixes, but in some special cases, the user may
     * define this.
     */
    private String prefix;
    /**
     * Name to use for unknown keys, i.e. key references that could not be resolved to an actual type in the database.
     */
    private final String unknownKeyName;
    /**
     * In addition to limit, this type of query supports offsets.
     */
    private int offset;

    public IndexQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer, String indexName, String query) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;
        this.indexName = indexName;
        this.query = query;
        parameters = Lists.newArrayList();
        orders = Lists.newArrayList();
        unknownKeyName = tx.getGraph().getConfiguration().getUnknownIndexKeyName();
        this.offset = 0;
    }

    //################################################
    // Inspection Methods
    //################################################

    public String getIndex() {
        return indexName;
    }

    public Parameter[] getParameters() {
        return parameters.toArray(new Parameter[0]);
    }

    public String getQuery() {
        return query;
    }

    public ImmutableList<Parameter<Order>> getOrders() {
        return ImmutableList.copyOf(orders);
    }

    public int getOffset() {
        return offset;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public IndexQueryBuilder setElementIdentifier(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier), "Prefix may not be a blank string");
        this.prefix = identifier;
        return this;
    }

    public String getUnknownKeyName() {
        return unknownKeyName;
    }


    //################################################
    // Builder Methods
    //################################################

    @Override
    public IndexQueryBuilder offset(int offset) {
        Preconditions.checkArgument(offset >= 0, "Invalid offset provided: %s", offset);
        this.offset = offset;
        return this;
    }

    @Override
    public JanusGraphIndexQuery orderBy(String key, Order order) {
        Preconditions.checkArgument(key != null && order != null, "Need to specify and key and an order");
        orders.add(Parameter.of(key, order));
        return this;
    }

    @Override
    public IndexQueryBuilder limit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameter(Parameter para) {
        parameters.add(para);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Iterable<Parameter> paras) {
        Iterables.addAll(parameters, paras);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Parameter... paras) {
        for (Parameter para : paras) addParameter(para);
        return this;
    }

    private <E extends JanusGraphElement> Stream<Result<E>> execute(ElementCategory resultType) {
        if (tx.hasModifications()) {
            LOG.warn("Modifications in this transaction might not be accurately reflected in this index query: {}", query);
        }
        return serializer.executeQuery(this, resultType, tx.getBackendTransaction(), tx)
                .map(r -> (Result<E>) new ResultImpl<>(tx.getConversionFunction(resultType).apply(r.getResult()), r.getScore()))
                .filter(r -> !r.getElement().isRemoved());
    }

    private Long executeTotals(ElementCategory resultType) {
        this.setLimit(0);
        if (tx.hasModifications()) {
            LOG.warn("Modifications in this transaction might not be accurately reflected in this index query: {}", query);
        }
        return serializer.executeTotals(this, resultType, tx.getBackendTransaction(), tx);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphVertex>> vertices() {
        return new StreamIterable<>(vertexStream());
    }

    @Override
    public Stream<Result<JanusGraphVertex>> vertexStream() {
        setPrefixInternal(VERTEX_PREFIX);
        return execute(ElementCategory.VERTEX);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphEdge>> edges() {
        return new StreamIterable<>(edgeStream());
    }

    @Override
    public Stream<Result<JanusGraphEdge>> edgeStream() {
        setPrefixInternal(EDGE_PREFIX);
        return execute(ElementCategory.EDGE);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphVertexProperty>> properties() {
        return new StreamIterable<>(propertyStream());
    }

    @Override
    public Stream<Result<JanusGraphVertexProperty>> propertyStream() {
        setPrefixInternal(PROPERTY_PREFIX);
        return execute(ElementCategory.PROPERTY);
    }

    @Override
    public Long vertexTotals() {
        setPrefixInternal(VERTEX_PREFIX);
        return executeTotals(ElementCategory.VERTEX);
    }

    @Override
    public Long edgeTotals() {
        setPrefixInternal(EDGE_PREFIX);
        return executeTotals(ElementCategory.EDGE);
    }

    @Override
    public Long propertyTotals() {
        setPrefixInternal(PROPERTY_PREFIX);
        return executeTotals(ElementCategory.PROPERTY);
    }

    private void setPrefixInternal(String prefix) {
        Preconditions.checkArgument(StringUtils.isNotBlank(prefix));
        if (this.prefix == null) this.prefix = prefix;
    }

    private static class ResultImpl<V extends Element> implements Result<V> {

        private final V element;
        private final double score;

        private ResultImpl(V element, double score) {
            this.element = element;
            this.score = score;
        }

        @Override
        public V getElement() {
            return element;
        }

        @Override
        public double getScore() {
            return score;
        }
    }
}

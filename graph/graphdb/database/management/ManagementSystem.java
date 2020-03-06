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

package grakn.core.graph.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.EdgeLabelMaker;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.JanusGraphSchemaElement;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.PropertyKeyMaker;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.core.schema.VertexLabelMaker;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.configuration.backend.KCVSConfiguration;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.internal.Token;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.ParameterIndexField;
import grakn.core.graph.graphdb.types.ParameterType;
import grakn.core.graph.graphdb.types.StandardEdgeLabelMaker;
import grakn.core.graph.graphdb.types.StandardPropertyKeyMaker;
import grakn.core.graph.graphdb.types.StandardRelationTypeMaker;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionMap;
import grakn.core.graph.graphdb.types.VertexLabelVertex;
import grakn.core.graph.graphdb.types.indextype.IndexTypeWrapper;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import grakn.core.graph.graphdb.types.vertices.PropertyKeyVertex;
import grakn.core.graph.graphdb.types.vertices.RelationTypeVertex;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static grakn.core.graph.graphdb.database.management.RelationTypeIndexWrapper.RELATION_INDEX_SEPARATOR;

public class ManagementSystem implements JanusGraphManagement {

    private final StandardJanusGraph graph;
    private final StandardJanusGraphTx transaction;

    private boolean isOpen;

    public ManagementSystem(StandardJanusGraph graph, KCVSConfiguration config) {
        this.graph = graph;
        this.transaction = graph.buildTransaction().disableBatchLoading().start();
        this.isOpen = true;
    }

    private void ensureOpen() {
        Preconditions.checkState(isOpen, "This management system instance has been closed");
    }

    @Override
    public synchronized void commit() {
        ensureOpen();

        //Commit underlying transaction
        transaction.commit();
        close();
    }

    @Override
    public synchronized void rollback() {
        ensureOpen();
        transaction.rollback();
        close();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    private void close() {
        isOpen = false;
    }

    private JanusGraphEdge addSchemaEdge(JanusGraphVertex out, JanusGraphVertex in, TypeDefinitionCategory def, Object modifier) {
        return transaction.addSchemaEdge(out, in, def, modifier);
    }

    // ###### INDEXING SYSTEM #####################

    /* --------------
    Type Indexes
     --------------- */

    public JanusGraphSchemaElement getSchemaElement(long id) {
        JanusGraphVertex v = transaction.getVertex(id);
        if (v == null) return null;
        if (v instanceof RelationType) {
            if (((InternalRelationType) v).getBaseType() == null) return (RelationType) v;
            return new RelationTypeIndexWrapper((InternalRelationType) v);
        }
        if (v instanceof JanusGraphSchemaVertex) {
            JanusGraphSchemaVertex sv = (JanusGraphSchemaVertex) v;
            if (sv.getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX)) {
                return new JanusGraphIndexWrapper(sv.asIndexType());
            }
        }
        throw new IllegalArgumentException("Not a valid schema element vertex: " + id);
    }

    @Override
    public RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, PropertyKey... sortKeys) {
        return buildRelationTypeIndex(label, name, direction, Order.ASC, sortKeys);
    }

    @Override
    public RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, org.apache.tinkerpop.gremlin.process.traversal.Order sortOrder, PropertyKey... sortKeys) {
        return buildRelationTypeIndex(label, name, direction, Order.convert(sortOrder), sortKeys);
    }

    @Override
    public RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, PropertyKey... sortKeys) {
        return buildRelationTypeIndex(key, name, Direction.OUT, Order.ASC, sortKeys);
    }

    @Override
    public RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, org.apache.tinkerpop.gremlin.process.traversal.Order sortOrder, PropertyKey... sortKeys) {
        return buildRelationTypeIndex(key, name, Direction.OUT, Order.convert(sortOrder), sortKeys);
    }

    private RelationTypeIndex buildRelationTypeIndex(RelationType type, String name, Direction direction, Order sortOrder, PropertyKey... sortKeys) {
        Preconditions.checkArgument(type != null && direction != null && sortOrder != null && sortKeys != null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Name cannot be blank: %s", name);
        Token.verifyName(name);
        Preconditions.checkArgument(sortKeys.length > 0, "Need to specify sort keys");
        for (RelationType key : sortKeys) Preconditions.checkArgument(key != null, "Keys cannot be null");
        Preconditions.checkArgument(!(type instanceof EdgeLabel) || !((EdgeLabel) type).isUnidirected() || direction == Direction.OUT,
                "Can only index uni-directed labels in the out-direction: %s", type);
        Preconditions.checkArgument(!((InternalRelationType) type).multiplicity().isUnique(direction),
                "The relation type [%s] has a multiplicity or cardinality constraint in direction [%s] and can therefore not be indexed", type, direction);

        String composedName = composeRelationTypeIndexName(type, name);
        StandardRelationTypeMaker maker;
        if (type.isEdgeLabel()) {
            StandardEdgeLabelMaker lm = (StandardEdgeLabelMaker) transaction.makeEdgeLabel(composedName);
            lm.unidirected(direction);
            maker = lm;
        } else {
            StandardPropertyKeyMaker lm = (StandardPropertyKeyMaker) transaction.makePropertyKey(composedName);
            lm.dataType(((PropertyKey) type).dataType());
            maker = lm;
        }
        maker.status(type.isNew() ? SchemaStatus.ENABLED : SchemaStatus.INSTALLED);
        maker.invisible();
        maker.multiplicity(Multiplicity.MULTI);
        maker.sortKey(sortKeys);
        maker.sortOrder(sortOrder);

        //Compose signature
        long[] typeSig = ((InternalRelationType) type).getSignature();
        Set<PropertyKey> signature = new HashSet<>();
        for (long typeId : typeSig) signature.add(transaction.getExistingPropertyKey(typeId));
        for (RelationType sortType : sortKeys) signature.remove(sortType);
        if (!signature.isEmpty()) {
            PropertyKey[] sig = signature.toArray(new PropertyKey[0]);
            maker.signature(sig);
        }
        RelationType typeIndex = maker.make();
        addSchemaEdge(type, typeIndex, TypeDefinitionCategory.RELATIONTYPE_INDEX, null);
        return new RelationTypeIndexWrapper((InternalRelationType) typeIndex);
    }

    private static String composeRelationTypeIndexName(RelationType type, String name) {
        return String.valueOf(type.longId()) + RELATION_INDEX_SEPARATOR + name;
    }

    @Override
    public boolean containsRelationIndex(RelationType type, String name) {
        return getRelationIndex(type, name) != null;
    }

    @Override
    public RelationTypeIndex getRelationIndex(RelationType type, String name) {
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        String composedName = composeRelationTypeIndexName(type, name);

        //Don't use SchemaCache to make code more compact and since we don't need the extra performance here
        JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(transaction, BaseKey.SchemaName, JanusGraphSchemaCategory.getRelationTypeName(composedName)), null);
        if (v == null) return null;
        return new RelationTypeIndexWrapper((InternalRelationType) v);
    }

    @Override
    public Iterable<RelationTypeIndex> getRelationIndexes(RelationType type) {
        Preconditions.checkArgument(type instanceof InternalRelationType, "Invalid relation type provided: %s", type);
        return Iterables.transform(Iterables.filter(((InternalRelationType) type).getRelationIndexes(), internalRelationType -> !type.equals(internalRelationType)), RelationTypeIndexWrapper::new);
    }

    /* --------------
    Graph Indexes
     --------------- */

    public static IndexType getGraphIndexDirect(String name, StandardJanusGraphTx transaction) {
        JanusGraphSchemaVertex v = transaction.getSchemaVertex(JanusGraphSchemaCategory.GRAPHINDEX.getSchemaName(name));
        if (v == null) return null;
        return v.asIndexType();
    }

    @Override
    public boolean containsGraphIndex(String name) {
        return getGraphIndex(name) != null;
    }

    @Override
    public JanusGraphIndex getGraphIndex(String name) {
        IndexType index = getGraphIndexDirect(name, transaction);
        return index == null ? null : new JanusGraphIndexWrapper(index);
    }

    @Override
    public Iterable<JanusGraphIndex> getGraphIndexes(Class<? extends Element> elementType) {
        return StreamSupport.stream(QueryUtil.getVertices(transaction, BaseKey.SchemaCategory, JanusGraphSchemaCategory.GRAPHINDEX).spliterator(), false)
                .map(janusGraphVertex -> ((JanusGraphSchemaVertex) janusGraphVertex).asIndexType())
                .filter(indexType -> indexType.getElement().subsumedBy(elementType))
                .map(JanusGraphIndexWrapper::new)
                .collect(Collectors.toList());
    }

    private void checkIndexName(String indexName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        Preconditions.checkArgument(getGraphIndex(indexName) == null, "An index with name '%s' has already been defined", indexName);
    }

    private JanusGraphIndex createMixedIndex(String indexName, ElementCategory elementCategory, JanusGraphSchemaType constraint, String backingIndex) {
        Preconditions.checkArgument(graph.getIndexSerializer().containsIndex(backingIndex), "Unknown external index backend: %s", backingIndex);
        checkIndexName(indexName);

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX, false);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY, elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX, backingIndex);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME, indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY, Cardinality.LIST);
        def.setValue(TypeDefinitionCategory.STATUS, SchemaStatus.ENABLED);
        JanusGraphSchemaVertex indexVertex = transaction.makeSchemaVertex(JanusGraphSchemaCategory.GRAPHINDEX, indexName, def);

        Preconditions.checkArgument(constraint == null || (elementCategory.isValidConstraint(constraint) && constraint instanceof JanusGraphSchemaVertex));
        if (constraint != null) {
            addSchemaEdge(indexVertex, (JanusGraphSchemaVertex) constraint, TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT, null);
        }
        updateSchemaVertex(indexVertex);
        return new JanusGraphIndexWrapper(indexVertex.asIndexType());
    }

    @Override
    public void addIndexKey(JanusGraphIndex index, PropertyKey key, Parameter... parameters) {
        Preconditions.checkArgument(key != null && index instanceof JanusGraphIndexWrapper
                && !(key instanceof BaseKey), "Need to provide valid index and key");
        if (parameters == null) parameters = new Parameter[0];
        IndexType indexType = ((JanusGraphIndexWrapper) index).getBaseIndex();
        Preconditions.checkArgument(indexType instanceof MixedIndexType, "Can only add keys to an external index, not %s", index.name());
        Preconditions.checkArgument(indexType instanceof IndexTypeWrapper && key instanceof JanusGraphSchemaVertex
                && ((IndexTypeWrapper) indexType).getSchemaBase() instanceof JanusGraphSchemaVertex);

        JanusGraphSchemaVertex indexVertex = (JanusGraphSchemaVertex) ((IndexTypeWrapper) indexType).getSchemaBase();

        for (IndexField field : indexType.getFieldKeys()) {
            Preconditions.checkArgument(!field.getFieldKey().equals(key), "Key [%s] has already been added to index %s", key.name(), index.name());
        }
        //Assemble parameters
        boolean addMappingParameter = !ParameterType.MAPPED_NAME.hasParameter(parameters);
        Parameter[] extendedParas = new Parameter[parameters.length + 1 + (addMappingParameter ? 1 : 0)];
        System.arraycopy(parameters, 0, extendedParas, 0, parameters.length);
        int arrPosition = parameters.length;
        if (addMappingParameter) {
            extendedParas[arrPosition++] = ParameterType.MAPPED_NAME.getParameter(graph.getIndexSerializer().getDefaultFieldName(key, parameters, indexType.getBackingIndexName()));
        }
        extendedParas[arrPosition] = ParameterType.STATUS.getParameter(key.isNew() ? SchemaStatus.ENABLED : SchemaStatus.INSTALLED);

        addSchemaEdge(indexVertex, key, TypeDefinitionCategory.INDEX_FIELD, extendedParas);
        updateSchemaVertex(indexVertex);
        indexType.resetCache();
        //Check to see if the index supports this
        if (!graph.getIndexSerializer().supports((MixedIndexType) indexType, ParameterIndexField.of(key, parameters))) {
            throw new JanusGraphException("Could not register new index field '" + key.name() + "' with index backend as the data type, cardinality or parameter combination is not supported.");
        }

        try {
            IndexSerializer.register((MixedIndexType) indexType, key, transaction.getBackendTransaction());
        } catch (BackendException e) {
            throw new JanusGraphException("Could not register new index field with index backend", e);
        }
    }

    private JanusGraphIndex createCompositeIndex(String indexName, ElementCategory elementCategory, boolean unique, JanusGraphSchemaType constraint, PropertyKey... keys) {
        checkIndexName(indexName);
        Preconditions.checkArgument(keys != null && keys.length > 0, "Need to provide keys to index [%s]", indexName);
        Preconditions.checkArgument(!unique || elementCategory == ElementCategory.VERTEX, "Unique indexes can only be created on vertices [%s]", indexName);
        boolean allSingleKeys = true;
        boolean oneNewKey = false;
        for (PropertyKey key : keys) {
            Preconditions.checkArgument(key instanceof PropertyKeyVertex, "Need to provide valid keys: %s", key);
            if (key.cardinality() != Cardinality.SINGLE) allSingleKeys = false;
            if (key.isNew()) oneNewKey = true;
        }

        Cardinality indexCardinality;
        if (unique) indexCardinality = Cardinality.SINGLE;
        else indexCardinality = (allSingleKeys ? Cardinality.SET : Cardinality.LIST);

        boolean canIndexBeEnabled = oneNewKey || (constraint != null && constraint.isNew());

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX, true);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY, elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX, Token.INTERNAL_INDEX_NAME);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME, indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY, indexCardinality);
        def.setValue(TypeDefinitionCategory.STATUS, canIndexBeEnabled ? SchemaStatus.ENABLED : SchemaStatus.INSTALLED);
        JanusGraphSchemaVertex indexVertex = transaction.makeSchemaVertex(JanusGraphSchemaCategory.GRAPHINDEX, indexName, def);
        for (int i = 0; i < keys.length; i++) {
            Parameter[] paras = {ParameterType.INDEX_POSITION.getParameter(i)};
            addSchemaEdge(indexVertex, keys[i], TypeDefinitionCategory.INDEX_FIELD, paras);
        }

        Preconditions.checkArgument(constraint == null || (elementCategory.isValidConstraint(constraint) && constraint instanceof JanusGraphSchemaVertex));
        if (constraint != null) {
            addSchemaEdge(indexVertex, (JanusGraphSchemaVertex) constraint, TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT, null);
        }
        updateSchemaVertex(indexVertex);

        return new JanusGraphIndexWrapper(indexVertex.asIndexType());
    }

    @Override
    public JanusGraphManagement.IndexBuilder buildIndex(String indexName, Class<? extends Element> elementType) {
        return new IndexBuilder(indexName, ElementCategory.getByClazz(elementType));
    }

    private class IndexBuilder implements JanusGraphManagement.IndexBuilder {

        private final String indexName;
        private final ElementCategory elementCategory;
        private boolean unique = false;
        private JanusGraphSchemaType constraint = null;
        private final Map<PropertyKey, Parameter[]> keys = new HashMap<>();

        private IndexBuilder(String indexName, ElementCategory elementCategory) {
            this.indexName = indexName;
            this.elementCategory = elementCategory;
        }

        @Override
        public JanusGraphManagement.IndexBuilder addKey(PropertyKey key) {
            Preconditions.checkArgument(key instanceof PropertyKeyVertex, "Key must be a user defined key: %s", key);
            keys.put(key, null);
            return this;
        }

        @Override
        public JanusGraphManagement.IndexBuilder addKey(PropertyKey key, Parameter... parameters) {
            Preconditions.checkArgument(key instanceof PropertyKeyVertex, "Key must be a user defined key: %s", key);
            keys.put(key, parameters);
            return this;
        }

        @Override
        public JanusGraphManagement.IndexBuilder indexOnly(JanusGraphSchemaType schemaType) {
            Preconditions.checkNotNull(schemaType);
            Preconditions.checkArgument(elementCategory.isValidConstraint(schemaType), "Need to specify a valid schema type for this index definition: %s", schemaType);
            constraint = schemaType;
            return this;
        }

        @Override
        public JanusGraphManagement.IndexBuilder unique() {
            unique = true;
            return this;
        }

        @Override
        public JanusGraphIndex buildCompositeIndex() {
            Preconditions.checkArgument(!keys.isEmpty(), "Need to specify at least one key for the composite index");
            PropertyKey[] keyArr = new PropertyKey[keys.size()];
            int pos = 0;
            for (Map.Entry<PropertyKey, Parameter[]> entry : keys.entrySet()) {
                Preconditions.checkArgument(entry.getValue() == null, "Cannot specify parameters for composite index: %s", entry.getKey());
                keyArr[pos++] = entry.getKey();
            }
            return createCompositeIndex(indexName, elementCategory, unique, constraint, keyArr);
        }

        @Override
        public JanusGraphIndex buildMixedIndex(String backingIndex) {
            Preconditions.checkArgument(StringUtils.isNotBlank(backingIndex), "Need to specify backing index name");
            Preconditions.checkArgument(!unique, "An external index cannot be unique");

            JanusGraphIndex index = createMixedIndex(indexName, elementCategory, constraint, backingIndex);
            for (Map.Entry<PropertyKey, Parameter[]> entry : keys.entrySet()) {
                addIndexKey(index, entry.getKey(), entry.getValue());
            }
            return index;
        }
    }

    private void updateSchemaVertex(JanusGraphSchemaVertex schemaVertex) {
        transaction.updateSchemaVertex(schemaVertex);
    }

    /* --------------
    Type Modifiers
     --------------- */

    /**
     * Retrieves the consistency level for a schema element (types and internal indexes)
     */
    @Override
    public ConsistencyModifier getConsistency(JanusGraphSchemaElement element) {
        Preconditions.checkArgument(element != null);
        if (element instanceof RelationType) return ((InternalRelationType) element).getConsistencyModifier();
        else if (element instanceof JanusGraphIndex) {
            IndexType index = ((JanusGraphIndexWrapper) element).getBaseIndex();
            if (index.isMixedIndex()) return ConsistencyModifier.DEFAULT;
            return ((CompositeIndexType) index).getConsistencyModifier();
        } else return ConsistencyModifier.DEFAULT;
    }

    @Override
    public Duration getTTL(JanusGraphSchemaType type) {
        Preconditions.checkArgument(type != null);
        int ttl;
        if (type instanceof VertexLabelVertex) {
            ttl = ((VertexLabelVertex) type).getTTL();
        } else if (type instanceof RelationTypeVertex) {
            ttl = ((RelationTypeVertex) type).getTTL();
        } else {
            throw new IllegalArgumentException("given type does not support TTL: " + type.getClass());
        }
        return Duration.ofSeconds(ttl);
    }

    // ###### TRANSACTION PROXY #########

    @Override
    public boolean containsRelationType(String name) {
        return transaction.containsRelationType(name);
    }

    @Override
    public RelationType getRelationType(String name) {
        return transaction.getRelationType(name);
    }

    @Override
    public boolean containsPropertyKey(String name) {
        return transaction.containsPropertyKey(name);
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        return transaction.getPropertyKey(name);
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        return transaction.containsEdgeLabel(name);
    }

    @Override
    public EdgeLabel getOrCreateEdgeLabel(String name) {
        return transaction.getOrCreateEdgeLabel(name);
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name) {
        return transaction.getOrCreatePropertyKey(name);
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        return transaction.getEdgeLabel(name);
    }

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return transaction.makePropertyKey(name);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return transaction.makeEdgeLabel(name);
    }

    @Override
    public <T extends RelationType> Iterable<T> getRelationTypes(Class<T> clazz) {
        Preconditions.checkNotNull(clazz);
        Iterable<? extends JanusGraphVertex> types;
        if (PropertyKey.class.equals(clazz)) {
            types = QueryUtil.getVertices(transaction, BaseKey.SchemaCategory, JanusGraphSchemaCategory.PROPERTYKEY);
        } else if (EdgeLabel.class.equals(clazz)) {
            types = QueryUtil.getVertices(transaction, BaseKey.SchemaCategory, JanusGraphSchemaCategory.EDGELABEL);
        } else if (RelationType.class.equals(clazz)) {
            types = Iterables.concat(getRelationTypes(EdgeLabel.class), getRelationTypes(PropertyKey.class));
        } else {
            throw new IllegalArgumentException("Unknown type class: " + clazz);
        }
        return Iterables.filter(Iterables.filter(types, clazz), t -> {
            //Filter out all relation type indexes
            return ((InternalRelationType) t).getBaseType() == null;
        });
    }

    @Override
    public boolean containsVertexLabel(String name) {
        return transaction.containsVertexLabel(name);
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        return transaction.getVertexLabel(name);
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        return transaction.getOrCreateVertexLabel(name);
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        return transaction.makeVertexLabel(name);
    }

    @Override
    public VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys) {
        return transaction.addProperties(vertexLabel, keys);
    }

    @Override
    public EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys) {
        return transaction.addProperties(edgeLabel, keys);
    }

    @Override
    public EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel) {
        return transaction.addConnection(edgeLabel, outVLabel, inVLabel);
    }

    @Override
    public Iterable<VertexLabel> getVertexLabels() {
        return Iterables.filter(QueryUtil.getVertices(transaction, BaseKey.SchemaCategory,
                JanusGraphSchemaCategory.VERTEXLABEL), VertexLabel.class);
    }
}

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

package grakn.core.graph.graphdb.database;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.MetaAnnotatable;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.indexing.IndexEntry;
import grakn.core.graph.diskstorage.indexing.IndexFeatures;
import grakn.core.graph.diskstorage.indexing.IndexInformation;
import grakn.core.graph.diskstorage.indexing.IndexProvider;
import grakn.core.graph.diskstorage.indexing.IndexQuery;
import grakn.core.graph.diskstorage.indexing.KeyInformation;
import grakn.core.graph.diskstorage.indexing.RawQuery;
import grakn.core.graph.diskstorage.indexing.StandardKeyInformation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.HashingUtil;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.management.ManagementSystem;
import grakn.core.graph.graphdb.database.serialize.AttributeUtil;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.query.condition.ConditionUtil;
import grakn.core.graph.graphdb.query.condition.PredicateCondition;
import grakn.core.graph.graphdb.query.graph.GraphCentricQueryBuilder;
import grakn.core.graph.graphdb.query.graph.IndexQueryBuilder;
import grakn.core.graph.graphdb.query.graph.JointIndexQuery;
import grakn.core.graph.graphdb.query.graph.MultiKeySliceQuery;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.graphdb.relations.RelationIdentifier;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.ParameterIndexField;
import grakn.core.graph.graphdb.types.ParameterType;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME_MAPPING;


public class IndexSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSerializer.class);

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    private final Serializer serializer;
    private final Configuration configuration;
    private final Map<String, ? extends IndexInformation> mixedIndexes;

    private final boolean hashKeys;
    private final HashingUtil.HashLength hashLength = HashingUtil.HashLength.SHORT;

    public IndexSerializer(Configuration config, Serializer serializer, Map<String, ? extends IndexInformation> indexes, boolean hashKeys) {
        this.serializer = serializer;
        this.configuration = config;
        this.mixedIndexes = indexes;
        this.hashKeys = hashKeys;
        if (hashKeys) LOG.debug("Hashing index keys");
    }


    /* ################################################
               Index Information
    ################################################### */

    public boolean containsIndex(String indexName) {
        return mixedIndexes.containsKey(indexName);
    }

    public String getDefaultFieldName(PropertyKey key, Parameter[] parameters, String indexName) {
        Preconditions.checkArgument(!ParameterType.MAPPED_NAME.hasParameter(parameters), "A field name mapping has been specified for key: %s", key);
        Preconditions.checkArgument(containsIndex(indexName), "Unknown backing index: %s", indexName);
        String fieldname = configuration.get(INDEX_NAME_MAPPING, indexName) ? key.name() : keyID2Name(key);
        return mixedIndexes.get(indexName).mapKey2Field(fieldname, new StandardKeyInformation(key, parameters));
    }

    public static void register(MixedIndexType index, PropertyKey key, BackendTransaction tx) throws BackendException {
        tx.getIndexTransaction(index.getBackingIndexName()).register(index.getStoreName(), key2Field(index, key), getKeyInformation(index.getField(key)));
    }

    public boolean supports(MixedIndexType index, ParameterIndexField field) {
        return getMixedIndex(index).supports(getKeyInformation(field));
    }

    public boolean supports(MixedIndexType index, ParameterIndexField field, JanusGraphPredicate predicate) {
        return getMixedIndex(index).supports(getKeyInformation(field), predicate);
    }

    public IndexFeatures features(MixedIndexType index) {
        return getMixedIndex(index).getFeatures();
    }

    private IndexInformation getMixedIndex(MixedIndexType index) {
        IndexInformation indexinfo = mixedIndexes.get(index.getBackingIndexName());
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: " + index.getBackingIndexName());
        return indexinfo;
    }

    private static StandardKeyInformation getKeyInformation(ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey(), field.getParameters());
    }

    public IndexInfoRetriever getIndexInfoRetriever(StandardJanusGraphTx tx) {
        return new IndexInfoRetriever(tx);
    }

    public static class IndexInfoRetriever implements KeyInformation.Retriever {

        private final StandardJanusGraphTx transaction;

        private IndexInfoRetriever(StandardJanusGraphTx tx) {
            transaction = tx;
        }

        @Override
        public KeyInformation.IndexRetriever get(String index) {
            return new KeyInformation.IndexRetriever() {

                final Map<String, KeyInformation.StoreRetriever> indexes = new ConcurrentHashMap<>();

                @Override
                public KeyInformation get(String store, String key) {
                    return get(store).get(key);
                }

                @Override
                public KeyInformation.StoreRetriever get(String store) {
                    if (indexes.get(store) == null) {
                        MixedIndexType extIndex = getMixedIndex(store, transaction);
                        ImmutableMap.Builder<String, KeyInformation> b = ImmutableMap.builder();
                        for (ParameterIndexField field : extIndex.getFieldKeys()) {
                            b.put(key2Field(field), getKeyInformation(field));
                        }
                        ImmutableMap<String, KeyInformation> infoMap = b.build();
                        KeyInformation.StoreRetriever storeRetriever = infoMap::get;
                        indexes.put(store, storeRetriever);
                    }
                    return indexes.get(store);
                }

            };
        }
    }

    /* ################################################
               Index Updates
    ################################################### */

    public static class IndexUpdate<K, E> {

        private enum Type {ADD, DELETE}

        private final IndexType index;
        private final Type mutationType;
        private final K key;
        private final E entry;
        private final JanusGraphElement element;

        private IndexUpdate(IndexType index, Type mutationType, K key, E entry, JanusGraphElement element) {
            this.index = index;
            this.mutationType = mutationType;
            this.key = key;
            this.entry = entry;
            this.element = element;
        }

        public JanusGraphElement getElement() {
            return element;
        }

        public IndexType getIndex() {
            return index;
        }

        public Type getType() {
            return mutationType;
        }

        public K getKey() {
            return key;
        }

        public E getEntry() {
            return entry;
        }

        public boolean isAddition() {
            return mutationType == Type.ADD;
        }

        public boolean isDeletion() {
            return mutationType == Type.DELETE;
        }

        public boolean isCompositeIndex() {
            return index.isCompositeIndex();
        }

        public boolean isMixedIndex() {
            return index.isMixedIndex();
        }

        public void setTTL(int ttl) {
            Preconditions.checkArgument(ttl > 0 && mutationType == Type.ADD);
            ((MetaAnnotatable) entry).setMetaData(EntryMetaData.TTL, ttl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, mutationType, key, entry);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (!(other instanceof IndexUpdate)) {
                return false;
            }
            IndexUpdate oth = (IndexUpdate) other;
            return index.equals(oth.index) && mutationType == oth.mutationType && key.equals(oth.key) && entry.equals(oth.entry);
        }
    }

    private static IndexUpdate.Type getUpdateType(InternalRelation relation) {
        return (relation.isNew() ? IndexUpdate.Type.ADD : IndexUpdate.Type.DELETE);
    }

    private static boolean indexAppliesTo(IndexType index, JanusGraphElement element) {
        return index.getElement().isInstance(element) &&
                (!(index instanceof CompositeIndexType) || ((CompositeIndexType) index).getStatus() != SchemaStatus.DISABLED) &&
                (!index.hasSchemaTypeConstraint() ||
                        index.getElement().matchesConstraint(index.getSchemaTypeConstraint(), element));
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalRelation relation) {
        Set<IndexUpdate> updates = Sets.newHashSet();
        IndexUpdate.Type updateType = getUpdateType(relation);
        int ttl = updateType == IndexUpdate.Type.ADD ? StandardJanusGraph.getTTL(relation) : 0;
        for (RelationType type : relation.getPropertyKeysDirect()) {
            if (!(type instanceof PropertyKey)) continue;
            PropertyKey key = (PropertyKey) type;
            for (IndexType index : ((InternalRelationType) key).getKeyIndexes()) {
                if (!indexAppliesTo(index, relation)) continue;
                IndexUpdate update;
                if (index instanceof CompositeIndexType) {
                    CompositeIndexType iIndex = (CompositeIndexType) index;
                    RecordEntry[] record = indexMatch(relation, iIndex);
                    if (record == null) continue;
                    update = new IndexUpdate<>(iIndex, updateType, getIndexKey(iIndex, record), getIndexEntry(iIndex, record, relation), relation);
                } else {
                    if (((MixedIndexType) index).getField(key).getStatus() == SchemaStatus.DISABLED) continue;
                    update = getMixedIndexUpdate(relation, key, relation.valueOrNull(key), (MixedIndexType) index, updateType);
                }
                if (ttl > 0) update.setTTL(ttl);
                updates.add(update);
            }
        }
        return updates;
    }

    private static PropertyKey[] getKeysOfRecords(RecordEntry[] record) {
        PropertyKey[] keys = new PropertyKey[record.length];
        for (int i = 0; i < record.length; i++) keys[i] = record[i].key;
        return keys;
    }

    private static int getIndexTTL(InternalVertex vertex, PropertyKey... keys) {
        int ttl = StandardJanusGraph.getTTL(vertex);
        for (PropertyKey key : keys) {
            int kttl = ((InternalRelationType) key).getTTL();
            if (kttl > 0 && (kttl < ttl || ttl <= 0)) ttl = kttl;
        }
        return ttl;
    }

    Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedProperties) {
        if (updatedProperties.isEmpty()) return Collections.emptyList();
        Set<IndexUpdate> updates = Sets.newHashSet();
        for (InternalRelation rel : updatedProperties) {
            JanusGraphVertexProperty p = (JanusGraphVertexProperty) rel;
            IndexUpdate.Type updateType = getUpdateType(rel);
            for (IndexType index : ((InternalRelationType) p.propertyKey()).getKeyIndexes()) {
                if (!indexAppliesTo(index, vertex)) continue;
                if (index.isCompositeIndex()) { //Gather composite indexes
                    CompositeIndexType cIndex = (CompositeIndexType) index;
                    IndexRecords updateRecords = indexMatches(vertex, cIndex, updateType == IndexUpdate.Type.DELETE, p.propertyKey(), new RecordEntry(p));
                    for (RecordEntry[] record : updateRecords) {
                        IndexUpdate update = new IndexUpdate<>(cIndex, updateType, getIndexKey(cIndex, record), getIndexEntry(cIndex, record, vertex), vertex);
                        int ttl = getIndexTTL(vertex, getKeysOfRecords(record));
                        if (ttl > 0 && updateType == IndexUpdate.Type.ADD) update.setTTL(ttl);
                        updates.add(update);
                    }
                } else { //Update mixed indexes
                    if (((MixedIndexType) index).getField(p.propertyKey()).getStatus() != SchemaStatus.DISABLED) {
                        IndexUpdate update = getMixedIndexUpdate(vertex, p.propertyKey(), p.value(), (MixedIndexType) index, updateType);
                        int ttl = getIndexTTL(vertex, p.propertyKey());
                        if (ttl > 0 && updateType == IndexUpdate.Type.ADD) update.setTTL(ttl);
                        updates.add(update);
                    }
                }
            }
        }
        return updates;
    }

    private IndexUpdate<String, IndexEntry> getMixedIndexUpdate(JanusGraphElement element, PropertyKey key, Object value, MixedIndexType index, IndexUpdate.Type updateType) {
        return new IndexUpdate<>(index, updateType, element2String(element), new IndexEntry(key2Field(index.getField(key)), value), element);
    }

    public void reindexElement(JanusGraphElement element, MixedIndexType index, Map<String, Map<String, List<IndexEntry>>> documentsPerStore) {
        if (!indexAppliesTo(index, element)) return;
        List<IndexEntry> entries = Lists.newArrayList();
        for (ParameterIndexField field : index.getFieldKeys()) {
            PropertyKey key = field.getFieldKey();
            if (field.getStatus() == SchemaStatus.DISABLED) continue;
            if (element.properties(key.name()).hasNext()) {
                element.values(key.name()).forEachRemaining(value -> entries.add(new IndexEntry(key2Field(field), value)));
            }
        }
        Map<String, List<IndexEntry>> documents = documentsPerStore.computeIfAbsent(index.getStoreName(), k -> Maps.newHashMap());
        getDocuments(documentsPerStore, index).put(element2String(element), entries);
    }

    private Map<String, List<IndexEntry>> getDocuments(Map<String, Map<String, List<IndexEntry>>> documentsPerStore, MixedIndexType index) {
        return documentsPerStore.computeIfAbsent(index.getStoreName(), k -> Maps.newHashMap());
    }

    public void removeElement(Object elementId, MixedIndexType index, Map<String, Map<String, List<IndexEntry>>> documentsPerStore) {
        Preconditions.checkArgument((index.getElement() == ElementCategory.VERTEX && elementId instanceof Long) ||
                (index.getElement().isRelation() && elementId instanceof RelationIdentifier), "Invalid element id [%s] provided for index: %s", elementId, index);
        getDocuments(documentsPerStore, index).put(element2String(elementId), Lists.newArrayList());
    }

    public Set<IndexUpdate<StaticBuffer, Entry>> reindexElement(JanusGraphElement element, CompositeIndexType index) {
        Set<IndexUpdate<StaticBuffer, Entry>> indexEntries = Sets.newHashSet();
        if (!indexAppliesTo(index, element)) return indexEntries;
        Iterable<RecordEntry[]> records;
        if (element instanceof JanusGraphVertex) records = indexMatches((JanusGraphVertex) element, index);
        else {
            records = Collections.EMPTY_LIST;
            RecordEntry[] record = indexMatch((JanusGraphRelation) element, index);
            if (record != null) records = ImmutableList.of(record);
        }
        for (RecordEntry[] record : records) {
            indexEntries.add(new IndexUpdate<>(index, IndexUpdate.Type.ADD, getIndexKey(index, record), getIndexEntry(index, record, element), element));
        }
        return indexEntries;
    }

    private static RecordEntry[] indexMatch(JanusGraphRelation relation, CompositeIndexType index) {
        IndexField[] fields = index.getFieldKeys();
        RecordEntry[] match = new RecordEntry[fields.length];
        for (int i = 0; i < fields.length; i++) {
            IndexField f = fields[i];
            Object value = relation.valueOrNull(f.getFieldKey());
            if (value == null) return null; //No match
            match[i] = new RecordEntry(relation.longId(), value, f.getFieldKey());
        }
        return match;
    }

    public static class IndexRecords extends ArrayList<RecordEntry[]> {

        @Override
        public boolean add(RecordEntry[] record) {
            return super.add(Arrays.copyOf(record, record.length));
        }

        private static Object[] getValues(RecordEntry[] record) {
            Object[] values = new Object[record.length];
            for (int i = 0; i < values.length; i++) {
                values[i] = record[i].value;
            }
            return values;
        }

    }

    private static class RecordEntry {

        final long relationId;
        final Object value;
        final PropertyKey key;

        private RecordEntry(long relationId, Object value, PropertyKey key) {
            this.relationId = relationId;
            this.value = value;
            this.key = key;
        }

        private RecordEntry(JanusGraphVertexProperty property) {
            this(property.longId(), property.value(), property.propertyKey());
        }
    }

    private static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index) {
        return indexMatches(vertex, index, null, null);
    }

    private static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index, PropertyKey replaceKey, Object replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFieldKeys();
        if (indexAppliesTo(index, vertex)) {
            indexMatches(vertex, new RecordEntry[fields.length], matches, fields, 0, false,
                    replaceKey, new RecordEntry(0, replaceValue, replaceKey));
        }
        return matches;
    }

    private static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                             boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex, new RecordEntry[fields.length], matches, fields, 0, onlyLoaded, replaceKey, replaceValue);
        return matches;
    }

    private static void indexMatches(JanusGraphVertex vertex, RecordEntry[] current, IndexRecords matches, IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        if (pos >= fields.length) {
            matches.add(current);
            return;
        }

        PropertyKey key = fields[pos].getFieldKey();

        List<RecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<>();
            Iterable<JanusGraphVertexProperty> props;
            if (onlyLoaded || (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.longId()))) {
                //going through transaction so we can query deleted vertices
                VertexCentricQueryBuilder qb = ((InternalVertex) vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties();
            } else {
                props = vertex.query().keys(key.name()).properties();
            }
            for (JanusGraphVertexProperty p : props) {
                values.add(new RecordEntry(p));
            }
        }
        for (RecordEntry value : values) {
            current[pos] = value;
            indexMatches(vertex, current, matches, fields, pos + 1, onlyLoaded, replaceKey, replaceValue);
        }
    }


    /* ################################################
                Querying
    ################################################### */

    public Stream<Object> query(JointIndexQuery.Subquery query, BackendTransaction tx) {
        IndexType index = query.getIndex();
        if (index.isCompositeIndex()) {
            MultiKeySliceQuery sq = query.getCompositeQuery();
            List<EntryList> rs = sq.execute(tx);
            List<Object> results = new ArrayList<>(rs.get(0).size());
            for (EntryList r : rs) {
                for (java.util.Iterator<Entry> iterator = r.reuseIterator(); iterator.hasNext(); ) {
                    Entry entry = iterator.next();
                    ReadBuffer entryValue = entry.asReadBuffer();
                    entryValue.movePositionTo(entry.getValuePosition());
                    if (index.getElement() == ElementCategory.VERTEX) {
                        results.add(VariableLong.readPositive(entryValue));
                    } else {
                        results.add(bytebuffer2RelationId(entryValue));
                    }
                }
            }
            return results.stream();
        } else {
            return tx.indexQuery(index.getBackingIndexName(), query.getMixedQuery()).map(IndexSerializer::string2ElementId);
        }
    }

    public MultiKeySliceQuery getQuery(CompositeIndexType index, List<Object[]> values) {
        List<KeySliceQuery> ksqs = new ArrayList<>(values.size());
        for (Object[] value : values) {
            ksqs.add(new KeySliceQuery(getIndexKey(index, value), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));
        }
        return new MultiKeySliceQuery(ksqs);
    }

    public IndexQuery getQuery(MixedIndexType index, Condition condition, OrderList orders) {
        Condition newCondition = ConditionUtil.literalTransformation(condition,
                new Function<Condition<JanusGraphElement>, Condition<JanusGraphElement>>() {
                    @Nullable
                    @Override
                    public Condition<JanusGraphElement> apply(Condition<JanusGraphElement> condition) {
                        Preconditions.checkArgument(condition instanceof PredicateCondition);
                        PredicateCondition pc = (PredicateCondition) condition;
                        PropertyKey key = (PropertyKey) pc.getKey();
                        return new PredicateCondition<>(key2Field(index, key), pc.getPredicate(), pc.getValue());
                    }
                });
        ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
        if (!orders.isEmpty() && GraphCentricQueryBuilder.indexCoversOrder(index, orders)) {
            ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
            for (int i = 0; i < orders.size(); i++) {
                lb.add(new IndexQuery.OrderEntry(key2Field(index, orders.getKey(i)), orders.getOrder(i), orders.getKey(i).dataType()));
            }
            newOrders = lb.build();
        }
        return new IndexQuery(index.getStoreName(), newCondition, newOrders);
    }

    // Common code used by executeQuery and executeTotals
    private String createQueryString(IndexQueryBuilder query, ElementCategory resultType, StandardJanusGraphTx transaction, MixedIndexType index) {
        Preconditions.checkArgument(index.getElement() == resultType, "Index is not configured for the desired result type: %s", resultType);
        String backingIndexName = index.getBackingIndexName();
        IndexProvider indexInformation = (IndexProvider) mixedIndexes.get(backingIndexName);

        StringBuilder qB = new StringBuilder(query.getQuery());
        String prefix = query.getPrefix();
        Preconditions.checkNotNull(prefix);
        //Convert query string by replacing
        int replacements = 0;
        int pos = 0;
        while (pos < qB.length()) {
            pos = qB.indexOf(prefix, pos);
            if (pos < 0) break;

            int startPos = pos;
            pos += prefix.length();
            StringBuilder keyBuilder = new StringBuilder();
            boolean quoteTerminated = qB.charAt(pos) == '"';
            if (quoteTerminated) pos++;
            while (pos < qB.length() && (Character.isLetterOrDigit(qB.charAt(pos)) || (quoteTerminated && qB.charAt(pos) != '"') || qB.charAt(pos) == '*')) {
                keyBuilder.append(qB.charAt(pos));
                pos++;
            }
            if (quoteTerminated) pos++;
            int endPos = pos;
            String keyName = keyBuilder.toString();
            Preconditions.checkArgument(StringUtils.isNotBlank(keyName),
                    "Found reference to empty key at position [%s]", startPos);
            String replacement;
            if (keyName.equals("*")) {
                replacement = indexInformation.getFeatures().getWildcardField();
            } else if (transaction.containsRelationType(keyName)) {
                PropertyKey key = transaction.getPropertyKey(keyName);
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                        "The used key [%s] is not indexed in the targeted index [%s]", key.name(), query.getIndex());
                replacement = key2Field(index, key);
            } else {
                Preconditions.checkArgument(query.getUnknownKeyName() != null,
                        "Found reference to non-existant property key in query at position [%s]: %s", startPos, keyName);
                replacement = query.getUnknownKeyName();
            }
            Preconditions.checkArgument(StringUtils.isNotBlank(replacement));

            qB.replace(startPos, endPos, replacement);
            pos = startPos + replacement.length();
            replacements++;
        }
        String queryStr = qB.toString();
        if (replacements <= 0) LOG.warn("Could not convert given {} index query: [{}]", resultType, query.getQuery());
        LOG.info("Converted query string with {} replacements: [{}] => [{}]", replacements, query.getQuery(), queryStr);
        return queryStr;
    }

    private ImmutableList<IndexQuery.OrderEntry> getOrders(IndexQueryBuilder query, ElementCategory resultType, StandardJanusGraphTx transaction, MixedIndexType index) {
        if (query.getOrders() == null) {
            return ImmutableList.of();
        }
        Preconditions.checkArgument(index.getElement() == resultType, "Index is not configured for the desired result type: %s", resultType);
        List<IndexQuery.OrderEntry> orderReplacement = new ArrayList<>();
        for (Parameter<Order> order : query.getOrders()) {
            if (transaction.containsRelationType(order.key())) {
                PropertyKey key = transaction.getPropertyKey(order.key());
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                        "The used key [%s] is not indexed in the targeted index [%s]", key.name(), query.getIndex());
                orderReplacement.add(new IndexQuery.OrderEntry(key2Field(index, key), grakn.core.graph.graphdb.internal.Order.convert(order.value()), key.dataType()));
            } else {
                Preconditions.checkArgument(query.getUnknownKeyName() != null,
                        "Found reference to non-existant property key in query orders %s", order.key());
            }
        }
        return ImmutableList.copyOf(orderReplacement);
    }

    public Stream<RawQuery.Result> executeQuery(IndexQueryBuilder query, ElementCategory resultType, BackendTransaction backendTx, StandardJanusGraphTx transaction) {
        MixedIndexType index = getMixedIndex(query.getIndex(), transaction);
        String queryStr = createQueryString(query, resultType, transaction, index);
        ImmutableList<IndexQuery.OrderEntry> orders = getOrders(query, resultType, transaction, index);
        RawQuery rawQuery = new RawQuery(index.getStoreName(), queryStr, orders, query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.rawQuery(index.getBackingIndexName(), rawQuery).map(result -> new RawQuery.Result(string2ElementId(result.getResult()), result.getScore()));
    }

    public Long executeTotals(IndexQueryBuilder query, ElementCategory resultType, BackendTransaction backendTx, StandardJanusGraphTx transaction) {
        MixedIndexType index = getMixedIndex(query.getIndex(), transaction);
        String queryStr = createQueryString(query, resultType, transaction, index);
        RawQuery rawQuery = new RawQuery(index.getStoreName(), queryStr, query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.totals(index.getBackingIndexName(), rawQuery);
    }

    /* ################################################
                Utility Functions
    ################################################### */

    private static MixedIndexType getMixedIndex(String indexName, StandardJanusGraphTx transaction) {
        IndexType index = ManagementSystem.getGraphIndexDirect(indexName, transaction);
        Preconditions.checkArgument(index != null, "Index with name [%s] is unknown or not configured properly", indexName);
        Preconditions.checkArgument(index.isMixedIndex());
        return (MixedIndexType) index;
    }

    private static String element2String(JanusGraphElement element) {
        return element2String(element.id());
    }

    private static String element2String(Object elementId) {
        Preconditions.checkArgument(elementId instanceof Long || elementId instanceof RelationIdentifier);
        if (elementId instanceof Long) return Long.toString((Long) elementId);
        else return ((RelationIdentifier) elementId).toString();
    }

    private static Object string2ElementId(String str) {
        if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) return RelationIdentifier.parse(str);
        else return Long.parseLong(str);
    }

    private static String key2Field(MixedIndexType index, PropertyKey key) {
        return key2Field(index.getField(key));
    }

    private static String key2Field(ParameterIndexField field) {
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(), keyID2Name(field.getFieldKey()));
    }

    private static String keyID2Name(PropertyKey key) {
        return Long.toString(key.longId());
    }

    private StaticBuffer getIndexKey(CompositeIndexType index, RecordEntry[] record) {
        return getIndexKey(index, IndexRecords.getValues(record));
    }

    private StaticBuffer getIndexKey(CompositeIndexType index, Object[] values) {
        DataOutput out = serializer.getDataOutput(8 * DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.getID());
        IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length > 0 && fields.length == values.length);
        for (int i = 0; i < fields.length; i++) {
            IndexField f = fields[i];
            Object value = values[i];
            Preconditions.checkNotNull(value);
            if (AttributeUtil.hasGenericDataType(f.getFieldKey())) {
                out.writeClassAndObject(value);
            } else {
                out.writeObjectNotNull(value);
            }
        }
        StaticBuffer key = out.getStaticBuffer();
        if (hashKeys) key = HashingUtil.hashPrefixKey(hashLength, key);
        return key;
    }

    public long getIndexIdFromKey(StaticBuffer key) {
        if (hashKeys) key = HashingUtil.getKey(hashLength, key);
        return VariableLong.readPositive(key.asReadBuffer());
    }

    private Entry getIndexEntry(CompositeIndexType index, RecordEntry[] record, JanusGraphElement element) {
        DataOutput out = serializer.getDataOutput(1 + 8 + 8 * record.length + 4 * 8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality() != Cardinality.SINGLE) {
            VariableLong.writePositive(out, element.longId());
            if (index.getCardinality() != Cardinality.SET) {
                for (RecordEntry re : record) {
                    VariableLong.writePositive(out, re.relationId);
                }
            }
        }
        int valuePosition = out.getPosition();
        if (element instanceof JanusGraphVertex) {
            VariableLong.writePositive(out, element.longId());
        } else {
            RelationIdentifier rid = (RelationIdentifier) element.id();
            long[] longs = rid.getLongRepresentation();
            Preconditions.checkArgument(longs.length == 3 || longs.length == 4);
            for (long aLong : longs) VariableLong.writePositive(out, aLong);
        }
        return new StaticArrayEntry(out.getStaticBuffer(), valuePosition);
    }

    private static RelationIdentifier bytebuffer2RelationId(ReadBuffer b) {
        long[] relationId = new long[4];
        for (int i = 0; i < 3; i++) relationId[i] = VariableLong.readPositive(b);
        if (b.hasRemaining()) relationId[3] = VariableLong.readPositive(b);
        else relationId = Arrays.copyOfRange(relationId, 0, 3);
        return RelationIdentifier.get(relationId);
    }


}

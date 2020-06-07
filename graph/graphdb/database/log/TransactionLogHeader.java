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

package grakn.core.graph.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import grakn.core.graph.core.log.Change;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.HashingUtil;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.transaction.TransactionConfiguration;

import java.time.Instant;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionLogHeader {

    private final long transactionId;
    private final Instant txTimestamp;
    private final TimestampProvider times;
    private final StaticBuffer logKey;

    public TransactionLogHeader(long transactionId, Instant txTimestamp, TimestampProvider times) {
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        this.times = times;
        Preconditions.checkArgument(this.transactionId > 0);
        Preconditions.checkNotNull(this.txTimestamp);
        logKey = HashingUtil.hashPrefixKey(HashingUtil.HashLength.SHORT, BufferUtil.getLongBuffer(transactionId));
    }

    public long getId() {
        return transactionId;
    }

    public Instant getTimestamp() {
        return txTimestamp;
    }

    public StaticBuffer getLogKey() {
        return logKey;
    }

    public StaticBuffer serializeModifications(Serializer serializer, LogTxStatus status, StandardJanusGraphTx tx, Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations) {
        Preconditions.checkArgument(status == LogTxStatus.PRECOMMIT || status == LogTxStatus.USER_LOG);
        DataOutput out = serializeHeader(serializer, 256 + (addedRelations.size() + deletedRelations.size()) * 40, status, status == LogTxStatus.PRECOMMIT ? tx.getConfiguration() : null);
        logRelations(out, addedRelations, tx);
        logRelations(out, deletedRelations, tx);
        return out.getStaticBuffer();
    }

    private static void logRelations(DataOutput out, Collection<InternalRelation> relations, StandardJanusGraphTx tx) {
        VariableLong.writePositive(out, relations.size());
        for (InternalRelation rel : relations) {
            VariableLong.writePositive(out, rel.getVertex(0).longId());
            grakn.core.graph.diskstorage.Entry entry = tx.getEdgeSerializer().writeRelation(rel, 0, tx);
            BufferUtil.writeEntry(out, entry);
        }
    }

    public StaticBuffer serializePrimary(Serializer serializer, LogTxStatus status) {
        Preconditions.checkArgument(status == LogTxStatus.PRIMARY_SUCCESS || status == LogTxStatus.COMPLETE_SUCCESS);
        DataOutput out = serializeHeader(serializer, status);
        return out.getStaticBuffer();
    }

    public StaticBuffer serializeSecondary(Serializer serializer, LogTxStatus status, Map<String, Throwable> indexFailures, boolean userLogSuccess) {
        Preconditions.checkArgument(status == LogTxStatus.SECONDARY_SUCCESS || status == LogTxStatus.SECONDARY_FAILURE);
        DataOutput out = serializeHeader(serializer, status);
        if (status == LogTxStatus.SECONDARY_FAILURE) {
            out.putBoolean(userLogSuccess);
            out.putInt(indexFailures.size());
            for (String index : indexFailures.keySet()) {
                out.writeObjectNotNull(index);
            }
        }
        return out.getStaticBuffer();
    }

    private DataOutput serializeHeader(Serializer serializer, LogTxStatus status) {
        return serializeHeader(serializer, 30, status, new EnumMap<>(LogTxMeta.class));
    }

    private DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status, TransactionConfiguration txConfig) {
        EnumMap<LogTxMeta, Object> metaMap = new EnumMap<>(LogTxMeta.class);
        if (txConfig != null) {
            for (LogTxMeta meta : LogTxMeta.values()) {
                Object value = meta.getValue(txConfig);
                if (value != null) {
                    metaMap.put(meta, value);
                }
            }
        }
        return serializeHeader(serializer, capacity, status, metaMap);
    }


    private DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status, EnumMap<LogTxMeta, Object> meta) {
        Preconditions.checkArgument(status != null && meta != null, "Invalid status or meta");
        DataOutput out = serializer.getDataOutput(capacity);
        out.putLong(times.getTime(txTimestamp));
        VariableLong.writePositive(out, transactionId);
        out.writeObjectNotNull(status);

        Preconditions.checkArgument(meta.size() < Byte.MAX_VALUE, "Too much meta data: %s", meta.size());
        out.putByte(VariableLong.unsignedByte(meta.size()));
        for (Map.Entry<LogTxMeta, Object> metaEntry : meta.entrySet()) {
            out.putByte(VariableLong.unsignedByte(metaEntry.getKey().ordinal()));
            out.writeObjectNotNull(metaEntry.getValue());
        }
        return out;
    }

    public static Entry parse(StaticBuffer buffer, Serializer serializer, TimestampProvider times) {
        ReadBuffer read = buffer.asReadBuffer();
        Instant txTimestamp = times.getTime(read.getLong());
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read), txTimestamp, times);
        LogTxStatus status = serializer.readObjectNotNull(read, LogTxStatus.class);
        EnumMap<LogTxMeta, Object> metadata = new EnumMap<>(LogTxMeta.class);
        int metaSize = VariableLong.unsignedByte(read.getByte());
        for (int i = 0; i < metaSize; i++) {
            LogTxMeta meta = LogTxMeta.values()[VariableLong.unsignedByte(read.getByte())];
            metadata.put(meta, serializer.readObjectNotNull(read, meta.dataType()));
        }
        if (read.hasRemaining()) {
            StaticBuffer content = read.subrange(read.getPosition(), read.length() - read.getPosition());
            return new Entry(header, content, status, metadata);
        } else {
            return new Entry(header, null, status, metadata);
        }
    }

    public static class Entry {
        private final TransactionLogHeader header;
        private final StaticBuffer content;
        private final LogTxStatus status;
        private final EnumMap<LogTxMeta, Object> metadata;

        public Entry(TransactionLogHeader header, StaticBuffer content, LogTxStatus status, EnumMap<LogTxMeta, Object> metadata) {
            Preconditions.checkArgument(status != null && metadata != null);
            Preconditions.checkArgument(header != null);
            Preconditions.checkArgument(content == null || content.length() > 0);
            this.header = header;
            this.content = content;
            this.status = status;
            this.metadata = metadata;
        }

        public TransactionLogHeader getHeader() {
            return header;
        }

        public boolean hasContent() {
            return content != null;
        }

        public LogTxStatus getStatus() {
            return status;
        }

        public EnumMap<LogTxMeta, Object> getMetadata() {
            return metadata;
        }

        public StaticBuffer getContent() {
            Preconditions.checkState(hasContent(), "Does not have any content");
            return content;
        }

        public SecondaryFailures getContentAsSecondaryFailures(Serializer serializer) {
            Preconditions.checkArgument(status == LogTxStatus.SECONDARY_FAILURE);
            return new SecondaryFailures(content, serializer);
        }

        public Collection<Modification> getContentAsModifications(Serializer serializer) {
            Preconditions.checkArgument(status == LogTxStatus.PRECOMMIT || status == LogTxStatus.USER_LOG);
            List<Modification> mods = Lists.newArrayList();
            ReadBuffer in = content.asReadBuffer();
            mods.addAll(readModifications(Change.ADDED, in, serializer));
            mods.addAll(readModifications(Change.REMOVED, in, serializer));
            return mods;
        }

        private static Collection<Modification> readModifications(Change state, ReadBuffer in, Serializer serializer) {
            List<Modification> mods = Lists.newArrayList();
            long size = VariableLong.readPositive(in);
            for (int i = 0; i < size; i++) {
                long vid = VariableLong.readPositive(in);
                grakn.core.graph.diskstorage.Entry entry = BufferUtil.readEntry(in, serializer);
                mods.add(new Modification(state, vid, entry));
            }
            return mods;
        }

    }

    public static class SecondaryFailures {
        public final boolean userLogFailure;
        public final Set<String> failedIndexes;

        private SecondaryFailures(StaticBuffer content, Serializer serializer) {
            ReadBuffer in = content.asReadBuffer();
            this.userLogFailure = !in.getBoolean();
            int size = in.getInt();
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            for (int i = 0; i < size; i++) {
                builder.add(serializer.readObjectNotNull(in, String.class));
            }
            this.failedIndexes = builder.build();
        }
    }

    public static class Modification {
        public final Change state;
        public final long outVertexId;
        public final grakn.core.graph.diskstorage.Entry relationEntry;

        private Modification(Change state, long outVertexId, grakn.core.graph.diskstorage.Entry relationEntry) {
            this.state = state;
            this.outVertexId = outVertexId;
            this.relationEntry = relationEntry;
        }
    }

}

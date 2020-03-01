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

package grakn.core.graph.graphdb.database.idassigner;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.IDAuthority;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import grakn.core.graph.graphdb.database.idassigner.placement.IDPlacementStrategy;
import grakn.core.graph.graphdb.database.idassigner.placement.PartitionAssignment;
import grakn.core.graph.graphdb.database.idassigner.placement.PartitionIDRange;
import grakn.core.graph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.relations.EdgeDirection;
import grakn.core.graph.graphdb.relations.ReassignableRelation;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import grakn.core.graph.util.stats.NumberUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.IDS_BLOCK_SIZE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.IDS_RENEW_BUFFER_PERCENTAGE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.IDS_RENEW_TIMEOUT;

public class VertexIDAssigner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(VertexIDAssigner.class);
    private static final int MAX_PARTITION_RENEW_ATTEMPTS = 1000;

    private final ConcurrentMap<Integer, PartitionIDPool> idPools;
    private final StandardIDPool schemaIdPool;
    private final StandardIDPool partitionVertexIdPool;

    private final IDAuthority idAuthority;
    private final IDManager idManager;
    private final IDPlacementStrategy placementStrategy;

    //For StandardIDPool
    private final Duration renewTimeoutMS;
    private final double renewBufferPercentage;
    private final int partitionIdBound;
    private final boolean hasLocalPartitions;

    public VertexIDAssigner(Configuration config, Backend backend) {
        long baseBlockSize = config.get(IDS_BLOCK_SIZE);
        SimpleVertexIDBlockSizer vertexIDBlockSizer = new SimpleVertexIDBlockSizer(baseBlockSize);
        this.idAuthority = new ConsistentKeyIDAuthority(backend.getIDsStore(), backend.getStoreManager(), config, vertexIDBlockSizer);

        int partitionBits = NumberUtil.getPowerOf2(config.get(CLUSTER_MAX_PARTITIONS));
        idManager = new IDManager(partitionBits);
        Preconditions.checkArgument(idManager.getPartitionBound() <= Integer.MAX_VALUE && idManager.getPartitionBound() > 0);
        partitionIdBound = (int) idManager.getPartitionBound();
        hasLocalPartitions = backend.getStoreFeatures().hasLocalKeyPartition();

        placementStrategy = new SimpleBulkPlacementStrategy(config);
        LOG.debug("Partition IDs? [{}], Local Partitions? [{}]", true, hasLocalPartitions);

        renewTimeoutMS = config.get(IDS_RENEW_TIMEOUT);
        renewBufferPercentage = config.get(IDS_RENEW_BUFFER_PERCENTAGE);

        idPools = new ConcurrentHashMap<>(partitionIdBound);
        schemaIdPool = new StandardIDPool(idAuthority, IDManager.SCHEMA_PARTITION, PoolType.SCHEMA.getIDNamespace(),
                IDManager.getSchemaCountBound(), renewTimeoutMS, renewBufferPercentage);
        partitionVertexIdPool = new StandardIDPool(idAuthority, IDManager.PARTITIONED_VERTEX_PARTITION, PoolType.PARTITIONED_VERTEX.getIDNamespace(),
                PoolType.PARTITIONED_VERTEX.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage);
        setLocalPartitions(partitionBits);
    }

    private void setLocalPartitionsToGlobal(int partitionBits) {
        placementStrategy.setLocalPartitionBounds(PartitionIDRange.getGlobalRange(partitionBits));
    }

    private void setLocalPartitions(int partitionBits) {
        if (!hasLocalPartitions) {
            setLocalPartitionsToGlobal(partitionBits);
        } else {
            List<PartitionIDRange> partitionRanges = ImmutableList.of();
            try {
                partitionRanges = PartitionIDRange.getIDRanges(partitionBits, idAuthority.getLocalIDPartition());
            } catch (Throwable e) {
                LOG.error("Could not process local id partitions", e);
            }

            if (!partitionRanges.isEmpty()) {
                LOG.info("Setting individual partition bounds: {}", partitionRanges);
                placementStrategy.setLocalPartitionBounds(partitionRanges);
            } else {
                setLocalPartitionsToGlobal(partitionBits);
            }
        }
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public synchronized void close() {
        schemaIdPool.close();
        for (PartitionIDPool pool : idPools.values()) {
            pool.close();
        }
        idPools.clear();
        try {
            idAuthority.close();
        } catch (BackendException e) {
            LOG.warn("Exception while closing IdAuthority", e);
        }
    }

    public void assignID(InternalRelation relation) {
        assignID(relation, null);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        Preconditions.checkArgument(vertex != null && label != null);
        assignID(vertex, getVertexIDType(label));
    }


    private void assignID(InternalElement element, IDManager.VertexIDType vertexIDType) {
        for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS; attempt++) {
            long partitionID = -1;
            if (element instanceof JanusGraphSchemaVertex) {
                partitionID = IDManager.SCHEMA_PARTITION;
            } else if (element instanceof JanusGraphVertex) {
                if (vertexIDType == IDManager.VertexIDType.PartitionedVertex) {
                    partitionID = IDManager.PARTITIONED_VERTEX_PARTITION;
                } else {
                    partitionID = placementStrategy.getPartition(element);
                }
            } else if (element instanceof InternalRelation) {
                InternalRelation relation = (InternalRelation) element;
                if (attempt < relation.getLen()) { //On the first attempts, try to use partition of incident vertices
                    InternalVertex incident = relation.getVertex(attempt);
                    Preconditions.checkArgument(incident.hasId());
                    if (!IDManager.VertexIDType.PartitionedVertex.is(incident.longId()) || relation.isProperty()) {
                        partitionID = getPartitionID(incident);
                    } else {
                        continue;
                    }
                } else {
                    partitionID = placementStrategy.getPartition(element);
                }
            }
            try {
                assignID(element, partitionID, vertexIDType);
            } catch (IDPoolExhaustedException e) {
                continue; //try again on a different partition
            }

            /*
              The next block of code checks the added the relation for partitioned vertices as either end point. If such exists,
              we might have to assign the relation to a different representative of that partitioned vertex using the following logic:
              1) Properties are always assigned to the canonical representative
              2) Edges are assigned to the partition block of the non-partitioned vertex
               2a) unless the edge is unique in the direction away from the partitioned vertex in which case its assigned to the canonical representative
               2b) if both end vertices are partitioned, it is assigned to the partition to which the edge id hashes
             */
            //Check if we should assign a different representative of a potential partitioned vertex
            if (element instanceof InternalRelation) {
                InternalRelation relation = (InternalRelation) element;
                if (relation.isProperty() && isPartitionedAt(relation, 0)) {
                    //Always assign properties to the canonical representative of a partitioned vertex
                    InternalVertex vertex = relation.getVertex(0);
                    ((ReassignableRelation) relation).setVertexAt(0, vertex.tx().getInternalVertex(idManager.getCanonicalVertexId(vertex.longId())));
                } else if (relation.isEdge()) {
                    for (int pos = 0; pos < relation.getArity(); pos++) {
                        if (isPartitionedAt(relation, pos)) {
                            InternalVertex incident = relation.getVertex(pos);
                            long newPartition;
                            int otherPosition = (pos + 1) % 2;
                            if (((InternalRelationType) relation.getType()).multiplicity().isUnique(EdgeDirection.fromPosition(pos))) {
                                //If the relation is unique in the direction, we assign it to the canonical vertex...
                                newPartition = idManager.getPartitionId(idManager.getCanonicalVertexId(incident.longId()));
                            } else if (!isPartitionedAt(relation, otherPosition)) {
                                //...else, we assign it to the partition of the non-partitioned vertex...
                                newPartition = getPartitionID(relation.getVertex(otherPosition));
                            } else {
                                //...and if such does not exists (i.e. both end vertices are partitioned) we use the hash of the relation id
                                newPartition = idManager.getPartitionHashForId(relation.longId());
                            }
                            if (idManager.getPartitionId(incident.longId()) != newPartition) {
                                ((ReassignableRelation) relation).setVertexAt(pos, incident.tx().getOtherPartitionVertex(incident, newPartition));
                            }
                        }
                    }
                }
            }
            return;
        }
        throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
    }

    private boolean isPartitionedAt(InternalRelation relation, int position) {
        return idManager.isPartitionedVertex(relation.getVertex(position).longId());
    }

    public void assignIDs(Iterable<InternalRelation> addedRelations) {
        if (!placementStrategy.supportsBulkPlacement()) {
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assignID(vertex, getVertexIDType(vertex));
                    }
                }
                assignID(relation);
            }
        } else {
            //2) only assign ids to (user) vertices
            Map<InternalVertex, PartitionAssignment> assignments = new HashMap<>();
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        if (vertex.vertexLabel().isPartitioned()) {
                            assignID(vertex, getVertexIDType(vertex)); //Assign partitioned vertex ids immediately
                        } else {
                            assignments.put(vertex, PartitionAssignment.EMPTY);
                        }
                    }
                }
            }
            LOG.trace("Bulk id assignment for {} vertices", assignments.size());
            for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS && (assignments != null && !assignments.isEmpty()); attempt++) {
                placementStrategy.getPartitions(assignments);
                Map<InternalVertex, PartitionAssignment> leftOvers = null;
                Iterator<Map.Entry<InternalVertex, PartitionAssignment>> iterator = assignments.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<InternalVertex, PartitionAssignment> entry = iterator.next();
                    try {
                        assignID(entry.getKey(), entry.getValue().getPartitionID(), getVertexIDType(entry.getKey()));
                        Preconditions.checkArgument(entry.getKey().hasId());
                    } catch (IDPoolExhaustedException e) {
                        leftOvers = new HashMap<>();
                        leftOvers.put(entry.getKey(), PartitionAssignment.EMPTY);
                        break;
                    }
                }
                if (leftOvers != null) {
                    while (iterator.hasNext()) leftOvers.put(iterator.next().getKey(), PartitionAssignment.EMPTY);
                    LOG.debug("Exhausted ID Pool in bulk assignment. Left-over vertices {}", leftOvers.size());
                }
                assignments = leftOvers;
            }
            if (assignments != null && !assignments.isEmpty()) {
                throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
            }
            //3) assign ids to relations
            for (InternalRelation relation : addedRelations) {
                assignID(relation);
            }
        }
    }

    private long getPartitionID(InternalVertex v) {
        long vid = v.longId();
        if (IDManager.VertexIDType.Schema.is(vid)) return IDManager.SCHEMA_PARTITION;
        else return idManager.getPartitionId(vid);
    }

    private void assignID(InternalElement element, long partitionIDl, IDManager.VertexIDType userVertexIDType) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!element.hasId());
        Preconditions.checkArgument((element instanceof JanusGraphRelation) ^ (userVertexIDType != null));
        Preconditions.checkArgument(partitionIDl >= 0 && partitionIDl < partitionIdBound, partitionIDl);
        int partitionID = (int) partitionIDl;

        long count;
        if (element instanceof JanusGraphSchemaVertex) {
            Preconditions.checkArgument(partitionID == IDManager.SCHEMA_PARTITION);
            count = schemaIdPool.nextID();
        } else if (userVertexIDType == IDManager.VertexIDType.PartitionedVertex) {
            Preconditions.checkArgument(partitionID == IDManager.PARTITIONED_VERTEX_PARTITION);
            Preconditions.checkArgument(partitionVertexIdPool != null);
            count = partitionVertexIdPool.nextID();
        } else {
            PartitionIDPool partitionPool = idPools.get(partitionID);
            if (partitionPool == null) {
                partitionPool = new PartitionIDPool(partitionID, idAuthority, idManager, renewTimeoutMS, renewBufferPercentage);
                idPools.putIfAbsent(partitionID, partitionPool);
                partitionPool = idPools.get(partitionID);
            }
            Preconditions.checkNotNull(partitionPool);
            if (partitionPool.isExhausted()) {
                placementStrategy.exhaustedPartition(partitionID);
                throw new IDPoolExhaustedException("Exhausted id pool for partition: " + partitionID);
            }
            IDPool idPool;
            if (element instanceof JanusGraphRelation) {
                idPool = partitionPool.getPool(PoolType.RELATION);
            } else {
                idPool = partitionPool.getPool(PoolType.getPoolTypeFor(userVertexIDType));
            }
            try {
                count = idPool.nextID();
                partitionPool.accessed();
            } catch (IDPoolExhaustedException e) {
                LOG.debug("Pool exhausted for partition id {}", partitionID);
                placementStrategy.exhaustedPartition(partitionID);
                partitionPool.exhaustedIdPool();
                throw e;
            }
        }

        long elementId;
        if (element instanceof InternalRelation) {
            elementId = idManager.getRelationID(count, partitionID);
        } else if (element instanceof PropertyKey) {
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.UserPropertyKey, count);
        } else if (element instanceof EdgeLabel) {
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, count);
        } else if (element instanceof VertexLabel) {
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.VertexLabel, count);
        } else if (element instanceof JanusGraphSchemaVertex) {
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.GenericSchemaType, count);
        } else {
            elementId = idManager.getVertexID(count, partitionID, userVertexIDType);
        }

        Preconditions.checkArgument(elementId >= 0);
        element.setId(elementId);
    }

    private static IDManager.VertexIDType getVertexIDType(VertexLabel vertexLabel) {
        if (vertexLabel.isPartitioned()) {
            return IDManager.VertexIDType.PartitionedVertex;
        } else if (vertexLabel.isStatic()) {
            return IDManager.VertexIDType.UnmodifiableVertex;
        } else {
            return IDManager.VertexIDType.NormalVertex;
        }
    }

    private static IDManager.VertexIDType getVertexIDType(JanusGraphVertex v) {
        return getVertexIDType(v.vertexLabel());
    }

    private class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(long size) {
            Preconditions.checkArgument(size > 0 && size < Integer.MAX_VALUE);
            this.baseBlockSize = size;
        }

        @Override
        public long getBlockSize(int idNamespace) {
            switch (PoolType.getPoolType(idNamespace)) {
                case NORMAL_VERTEX:
                    return baseBlockSize;
                case UNMODIFIABLE_VERTEX:
                    return Math.max(10, baseBlockSize / 10);
                case PARTITIONED_VERTEX:
                    return Math.max(10, baseBlockSize / 100);
                case RELATION:
                    return baseBlockSize * 8;
                case SCHEMA:
                    return 50;
                default:
                    throw new IllegalArgumentException("Unrecognized pool type");
            }
        }

        @Override
        public long getIdUpperBound(int idNamespace) {
            return PoolType.getPoolType(idNamespace).getCountBound(idManager);
        }
    }

    private enum PoolType {

        NORMAL_VERTEX, UNMODIFIABLE_VERTEX, PARTITIONED_VERTEX, RELATION, SCHEMA;

        public int getIDNamespace() {
            return ordinal();
        }

        public long getCountBound(IDManager idManager) {
            switch (this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case PARTITIONED_VERTEX:
                    return idManager.getVertexCountBound();
                case RELATION:
                    return idManager.getRelationCountBound();
                case SCHEMA:
                    return IDManager.getSchemaCountBound();
                default:
                    throw new AssertionError("Unrecognized type: " + this);
            }
        }

        public boolean hasOnePerPartition() {
            switch (this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case RELATION:
                    return true;
                default:
                    return false;
            }
        }

        public static PoolType getPoolTypeFor(IDManager.VertexIDType idType) {
            if (idType == IDManager.VertexIDType.NormalVertex) {
                return NORMAL_VERTEX;
            } else if (idType == IDManager.VertexIDType.UnmodifiableVertex) {
                return UNMODIFIABLE_VERTEX;
            } else if (idType == IDManager.VertexIDType.PartitionedVertex) {
                return PARTITIONED_VERTEX;
            } else if (IDManager.VertexIDType.Schema.isSubType(idType)) {
                return SCHEMA;
            } else {
                throw new IllegalArgumentException("Invalid id type: " + idType);
            }
        }

        public static PoolType getPoolType(int idNamespace) {
            Preconditions.checkArgument(idNamespace >= 0 && idNamespace < values().length);
            return values()[idNamespace];
        }

    }

    private static class PartitionIDPool extends EnumMap<PoolType, IDPool> {

        private volatile long lastAccess;
        private volatile boolean exhausted;

        PartitionIDPool(int partitionID, IDAuthority idAuthority, IDManager idManager, Duration renewTimeoutMS, double renewBufferPercentage) {
            super(PoolType.class);
            for (PoolType type : PoolType.values()) {
                if (!type.hasOnePerPartition()) continue;
                put(type, new StandardIDPool(idAuthority, partitionID, type.getIDNamespace(), type.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage));
            }
        }

        IDPool getPool(PoolType type) {
            Preconditions.checkArgument(!exhausted && type.hasOnePerPartition());
            return super.get(type);
        }

        public void close() {
            for (IDPool pool : values()) pool.close();
            super.clear();
        }

        void exhaustedIdPool() {
            exhausted = true;
            close();
        }

        public boolean isExhausted() {
            return exhausted;
        }

        public void accessed() {
            lastAccess = System.currentTimeMillis();
        }

        public long getLastAccess() {
            return lastAccess;
        }

    }


}

/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.ResourceUnavailableException;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.Message;
import grakn.core.graph.diskstorage.log.MessageReader;
import grakn.core.graph.diskstorage.util.time.Timer;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.database.cache.SchemaCache;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static grakn.core.graph.graphdb.database.management.GraphCacheEvictionAction.DO_NOT_EVICT;
import static grakn.core.graph.graphdb.database.management.GraphCacheEvictionAction.EVICT;


public class ManagementLogger implements MessageReader {

    private static final Logger LOG = LoggerFactory.getLogger(ManagementLogger.class);

    private static final Duration SLEEP_INTERVAL = Duration.ofMillis(100L);
    private static final Duration MAX_WAIT_TIME = Duration.ofSeconds(60L);

    private final StandardJanusGraph graph;
    private final SchemaCache schemaCache;
    private final Log sysLog;
    private final TimestampProvider times;

    private final AtomicInteger evictionTriggerCounter = new AtomicInteger(0);
    private final ConcurrentMap<Long, EvictionTrigger> evictionTriggerMap = new ConcurrentHashMap<>();

    public ManagementLogger(StandardJanusGraph graph, Log sysLog, SchemaCache schemaCache, TimestampProvider times) {
        this.graph = graph;
        this.schemaCache = schemaCache;
        this.sysLog = sysLog;
        this.times = times;
    }

    @Override
    public void read(Message message) {
        ReadBuffer in = message.getContent().asReadBuffer();
        String senderId = message.getSenderId();
        Serializer serializer = graph.getDataSerializer();
        MgmtLogType logType = serializer.readObjectNotNull(in, MgmtLogType.class);
        Preconditions.checkNotNull(logType);
        switch (logType) {
            case CACHED_TYPE_EVICTION: {
                long evictionId = VariableLong.readPositive(in);
                long numEvictions = VariableLong.readPositive(in);
                for (int i = 0; i < numEvictions; i++) {
                    long typeId = VariableLong.readPositive(in);
                    schemaCache.expireSchemaElement(typeId);
                }
                GraphCacheEvictionAction action = serializer.readObjectNotNull(in, GraphCacheEvictionAction.class);
                Preconditions.checkNotNull(action);
                Thread ack = new Thread(new SendAckOnTxClose(evictionId, senderId, graph.getOpenTransactions(), action));
                ack.setDaemon(true);
                ack.start();
                break;
            }
            case CACHED_TYPE_EVICTION_ACK: {
                String receiverId = serializer.readObjectNotNull(in, String.class);
                long evictionId = VariableLong.readPositive(in);
                if (receiverId.equals(graph.getConfiguration().getUniqueGraphId())) {
                    //Acknowledgements targeted at this instance
                    EvictionTrigger evictTrigger = evictionTriggerMap.get(evictionId);
                    if (evictTrigger != null) {
                        evictTrigger.receivedAcknowledgement(senderId);
                    } else LOG.error("Could not find eviction trigger for {} from {}", evictionId, senderId);
                }

                break;
            }
            default:
                break;
        }

    }

    void sendCacheEviction(Set<JanusGraphSchemaVertex> updatedTypes, List<Callable<Boolean>> updatedTypeTriggers) {
        long evictionId = evictionTriggerCounter.incrementAndGet();
        evictionTriggerMap.put(evictionId, new EvictionTrigger(evictionId, updatedTypeTriggers, graph));
        DataOutput out = graph.getDataSerializer().getDataOutput(128);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out, evictionId);
        VariableLong.writePositive(out, updatedTypes.size());
        for (JanusGraphSchemaVertex type : updatedTypes) {
            VariableLong.writePositive(out, type.longId());
        }
        out.writeObjectNotNull(DO_NOT_EVICT);

        sysLog.add(out.getStaticBuffer());
    }

    @Override
    public void updateState() {
        evictionTriggerMap.forEach((k, v) -> {
            int ackCounter = v.removeDroppedInstances();
            if (ackCounter == 0) {
                v.runTriggers();
            }
        });
    }

    private class EvictionTrigger {

        final long evictionId;
        final List<Callable<Boolean>> updatedTypeTriggers;
        final StandardJanusGraph graph;
        final Set<String> instancesToBeAcknowledged;

        private EvictionTrigger(long evictionId, List<Callable<Boolean>> updatedTypeTriggers, StandardJanusGraph graph) {
            this.graph = graph;
            this.evictionId = evictionId;
            this.updatedTypeTriggers = updatedTypeTriggers;
            JanusGraphManagement mgmt = graph.openManagement();
            this.instancesToBeAcknowledged = ConcurrentHashMap.newKeySet();
            // do we need this? probably not bye
//            instancesToBeAcknowledged.addAll(((ManagementSystem) mgmt).getOpenInstancesInternal());
            mgmt.rollback();
        }

        void receivedAcknowledgement(String senderId) {
            if (instancesToBeAcknowledged.remove(senderId)) {
                int ackCounter = instancesToBeAcknowledged.size();
                LOG.debug("Received acknowledgement for eviction [{}] from senderID={} ({} more acks still outstanding)",
                        evictionId, senderId, ackCounter);
                if (ackCounter == 0) {
                    runTriggers();
                }
            }
        }

        void runTriggers() {
            for (Callable<Boolean> trigger : updatedTypeTriggers) {
                try {
                    trigger.call();
                } catch (Throwable e) {
                    LOG.error("Could not execute trigger [" + trigger.toString() + "] for eviction [" + evictionId + "]", e);
                }
            }
            LOG.debug("Received all acknowledgements for eviction [{}]", evictionId);
            evictionTriggerMap.remove(evictionId, this);
        }

        int removeDroppedInstances() {
            JanusGraphManagement mgmt = graph.openManagement();
            mgmt.rollback();
            return instancesToBeAcknowledged.size();
        }
    }

    private class SendAckOnTxClose implements Runnable {

        private final long evictionId;
        private final Set<? extends JanusGraphTransaction> openTx;
        private final String originId;
        private final GraphCacheEvictionAction action;

        private SendAckOnTxClose(long evictionId, String originId, Set<? extends JanusGraphTransaction> openTx, GraphCacheEvictionAction action) {
            this.evictionId = evictionId;
            this.openTx = openTx;
            this.originId = originId;
            this.action = action;
        }

        @Override
        public void run() {
            Timer t = times.getTimer().start();
            while (true) {
                boolean txStillOpen = false;
                Iterator<? extends JanusGraphTransaction> iterator = openTx.iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().isClosed()) {
                        iterator.remove();
                    } else {
                        txStillOpen = true;
                    }
                }
                boolean janusGraphManagerIsInBadState = action.equals(EVICT);
                if (!txStillOpen && janusGraphManagerIsInBadState) {
                    LOG.error("JanusGraphManager should be instantiated on this server, but it is not. " +
                            "Please restart with proper server settings. " +
                            "As a result, we could not evict graph from the cache.");
                    break;
                } else if (!txStillOpen) {
                    //Send ack and finish up
                    DataOutput out = graph.getDataSerializer().getDataOutput(64);
                    out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION_ACK);
                    out.writeObjectNotNull(originId);
                    VariableLong.writePositive(out, evictionId);
                    try {
                        sysLog.add(out.getStaticBuffer());
                        LOG.debug("Sent {}: evictionID={} originID={}", MgmtLogType.CACHED_TYPE_EVICTION_ACK, evictionId, originId);
                    } catch (ResourceUnavailableException e) {
                        //During shutdown, this event may be triggered but the LOG is already closed. The failure to send the acknowledgement
                        //can then be ignored
                        LOG.warn("System LOG has already shut down. Did not sent {}: evictionID={} originID={}", MgmtLogType.CACHED_TYPE_EVICTION_ACK, evictionId, originId);
                    }
                    break;
                }
                if (MAX_WAIT_TIME.compareTo(t.elapsed()) < 0) {
                    //Break out if waited too long
                    LOG.error("Evicted [{}] from cache but waiting too long for transactions to close. Stale transaction alert on: {}", getId(), openTx);
                    break;
                }
                try {
                    times.sleepPast(times.getTime().plus(SLEEP_INTERVAL));
                } catch (InterruptedException e) {
                    LOG.error("Interrupted eviction ack thread for " + getId(), e);
                    break;
                }
            }
        }

        public String getId() {
            return evictionId + "@" + originId;
        }
    }

}

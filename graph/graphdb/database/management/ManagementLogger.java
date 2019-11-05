// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider times;

    private final AtomicInteger evictionTriggerCounter = new AtomicInteger(0);
    private final ConcurrentMap<Long, EvictionTrigger> evictionTriggerMap = new ConcurrentHashMap<>();

    public ManagementLogger(StandardJanusGraph graph, Log sysLog, SchemaCache schemaCache, TimestampProvider times) {
        this.graph = graph;
        this.schemaCache = schemaCache;
        this.sysLog = sysLog;
        this.times = times;
        Preconditions.checkNotNull(times);
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
                Thread ack = new Thread(new SendAckOnTxClose(evictionId, senderId, graph.getOpenTransactions(), action, graph.getGraphName()));
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

    void sendCacheEviction(Set<JanusGraphSchemaVertex> updatedTypes, boolean evictGraphFromCache, List<Callable<Boolean>> updatedTypeTriggers, Set<String> openInstances) {
        Preconditions.checkArgument(!openInstances.isEmpty());
        long evictionId = evictionTriggerCounter.incrementAndGet();
        evictionTriggerMap.put(evictionId, new EvictionTrigger(evictionId, updatedTypeTriggers, graph));
        DataOutput out = graph.getDataSerializer().getDataOutput(128);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out, evictionId);
        VariableLong.writePositive(out, updatedTypes.size());
        for (JanusGraphSchemaVertex type : updatedTypes) {
            VariableLong.writePositive(out, type.longId());
        }
        if (evictGraphFromCache) {
            out.writeObjectNotNull(EVICT);
        } else {
            out.writeObjectNotNull(DO_NOT_EVICT);
        }
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
            final JanusGraphManagement mgmt = graph.openManagement();
            this.instancesToBeAcknowledged = ConcurrentHashMap.newKeySet();
            instancesToBeAcknowledged.addAll(((ManagementSystem) mgmt).getOpenInstancesInternal());
            mgmt.rollback();
        }

        void receivedAcknowledgement(String senderId) {
            if (instancesToBeAcknowledged.remove(senderId)) {
                final int ackCounter = instancesToBeAcknowledged.size();
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
            Set<String> updatedInstances = ((ManagementSystem) mgmt).getOpenInstancesInternal();
            String instanceRemovedMsg = "Instance [{}] was removed list of open instances and therefore dropped from list of instances to be acknowledged.";
            instancesToBeAcknowledged.stream().filter(it -> !updatedInstances.contains(it)).filter(instancesToBeAcknowledged::remove).forEach(it -> LOG.debug(instanceRemovedMsg, it));
            mgmt.rollback();
            return instancesToBeAcknowledged.size();
        }
    }

    private class SendAckOnTxClose implements Runnable {

        private final long evictionId;
        private final Set<? extends JanusGraphTransaction> openTx;
        private final String originId;
        private final GraphCacheEvictionAction action;
        private final String graphName;

        private SendAckOnTxClose(long evictionId, String originId, Set<? extends JanusGraphTransaction> openTx, GraphCacheEvictionAction action, String graphName) {
            this.evictionId = evictionId;
            this.openTx = openTx;
            this.originId = originId;
            this.action = action;
            this.graphName = graphName;
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
                            "As a result, we could not evict graph {} from the cache.", graphName);
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

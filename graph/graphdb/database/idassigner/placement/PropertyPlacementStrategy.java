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

package grakn.core.graph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.PreInitializeConfigOptions;
import grakn.core.graph.graphdb.database.idassigner.IDPoolExhaustedException;
import grakn.core.graph.graphdb.database.idassigner.placement.PartitionAssignment;
import grakn.core.graph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import grakn.core.graph.graphdb.database.idassigner.placement.SimplePartitionAssignment;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.internal.InternalVertex;

import java.util.Map;


@PreInitializeConfigOptions
public class PropertyPlacementStrategy extends SimpleBulkPlacementStrategy {

    public static final ConfigOption<String> PARTITION_KEY = new ConfigOption<String>(GraphDatabaseConfiguration.IDS_NS,
            "partition-key", "Partitions the graph by properties of this key", ConfigOption.Type.MASKABLE,
            String.class, StringUtils::isNotBlank);


    private String key;
    private IDManager idManager;

    public PropertyPlacementStrategy(Configuration config) {
        super(config);
        setPartitionKey(config.get(PARTITION_KEY));
    }

    public PropertyPlacementStrategy(String key, int concurrentPartitions) {
        super(concurrentPartitions);
        setPartitionKey(key);
    }

    private void setPartitionKey(String key) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "Invalid key configured: %s", key);
        this.key = key;
    }

    @Override
    public void injectIDManager(IDManager idManager) {
        Preconditions.checkNotNull(idManager);
        this.idManager = idManager;
    }


    @Override
    public int getPartition(InternalElement element) {
        if (element instanceof JanusGraphVertex) {
            int pid = getPartitionIDbyKey((JanusGraphVertex) element);
            if (pid >= 0) return pid;
        }
        return super.getPartition(element);
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        super.getPartitions(vertices);
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            int pid = getPartitionIDbyKey(entry.getKey());
            if (pid >= 0) ((SimplePartitionAssignment) entry.getValue()).setPartitionID(pid);
        }
    }

    private int getPartitionIDbyKey(JanusGraphVertex vertex) {
        Preconditions.checkState(idManager != null && key != null,
                "PropertyPlacementStrategy has not been initialized correctly");
        int partitionBound = (int) idManager.getPartitionBound();
        JanusGraphVertexProperty p = Iterables.getFirst(vertex.query().keys(key).properties(), null);
        if (p == null) return -1;
        int hashPid = Math.abs(p.value().hashCode()) % partitionBound;
        if (isExhaustedPartition(hashPid)) {
            //We keep trying consecutive partition ids until we find a non-exhausted one
            int newPid = hashPid;
            do {
                newPid = (newPid + 1) % partitionBound;
                if (newPid == hashPid) //We have gone full circle - no more ids to try
                    throw new IDPoolExhaustedException("Could not find non-exhausted partition");
            } while (isExhaustedPartition(newPid));
            return newPid;
        } else return hashPid;
    }
}

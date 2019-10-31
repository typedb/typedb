// Copyright 2019 JanusGraph Authors
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

package grakn.core.graph.diskstorage.configuration.backend.builder;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.time.TimestampProviders;

import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SETUP_WAITTIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_CONFIGURATION_IDENTIFIER;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Builder to build {@link KCVSConfiguration} instances:
 *
 * All KCVSConfigurations read from 'system_properties' Store
 */
public class KCVSConfigurationBuilder {

    /**
     * Build KCVSConfiguration concerning Global Configuration, system wide
     */
    public KCVSConfiguration buildGlobalConfiguration(BackendOperation.TransactionalProvider txProvider, KeyColumnValueStore store, Configuration config) {
        return buildConfiguration(txProvider, store, SYSTEM_CONFIGURATION_IDENTIFIER, config);
    }

    private KCVSConfiguration buildConfiguration(BackendOperation.TransactionalProvider txProvider, KeyColumnValueStore store, String identifier, Configuration config) {
        Duration setUpWaitingTime = config.get(SETUP_WAITTIME);
        TimestampProviders timestampProvider = config.get(TIMESTAMP_PROVIDER);
        Preconditions.checkArgument(Duration.ZERO.compareTo(setUpWaitingTime) < 0, "Wait time must be nonnegative: %s", setUpWaitingTime);

        return new KCVSConfiguration(txProvider, timestampProvider, setUpWaitingTime, store, identifier);
    }

}

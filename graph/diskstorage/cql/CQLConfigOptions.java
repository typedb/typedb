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

package grakn.core.graph.diskstorage.cql;

import grakn.core.graph.diskstorage.configuration.ConfigElement;
import grakn.core.graph.diskstorage.configuration.ConfigNamespace;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Configuration options for the CQL storage backend. These are managed under the 'cql' namespace in the configuration.
 */
public interface CQLConfigOptions {

    ConfigNamespace CQL_NS = new ConfigNamespace(
            GraphDatabaseConfiguration.STORAGE_NS,
            "cql",
            "CQL storage backend options");

    ConfigOption<String> KEYSPACE = new ConfigOption<>(
            CQL_NS,
            "keyspace",
            "The name of JanusGraph's keyspace.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL,
            "janusgraph");

    ConfigOption<Integer> PROTOCOL_VERSION = new ConfigOption<>(
            CQL_NS,
            "protocol-version",
            "The protocol version used to connect to the Cassandra database.  If no value is supplied then the driver will negotiate with the server.",
            ConfigOption.Type.LOCAL,
            0);

    ConfigOption<String> READ_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "read-consistency-level",
            "The consistency level of read operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    ConfigOption<String> WRITE_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "write-consistency-level",
            "The consistency level of write operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    // Whether to use un-logged batches
    ConfigOption<Boolean> ATOMIC_BATCH_MUTATE = new ConfigOption<>(
            CQL_NS,
            "atomic-batch-mutate",
            "True to use Cassandra atomic batch mutation, false to use non-atomic batches",
            ConfigOption.Type.MASKABLE,
            false);

    // Replication
    ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<>(
            CQL_NS,
            "replication-factor",
            "The number of data replicas (including the original copy) that should be kept",
            ConfigOption.Type.GLOBAL_OFFLINE,
            1);

    ConfigOption<String> REPLICATION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-class",
            "The replication strategy to use for JanusGraph keyspace",
            ConfigOption.Type.FIXED,
            "SimpleStrategy");

    ConfigOption<String[]> REPLICATION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set " +
                    "here takes precedence over one set with " + ConfigElement.getPath(REPLICATION_FACTOR),
            ConfigOption.Type.FIXED,
            String[].class);

    ConfigOption<String> COMPACTION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-class",
            "The compaction strategy to use for JanusGraph tables",
            ConfigOption.Type.FIXED,
            String.class);

    ConfigOption<String[]> COMPACTION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-options",
            "Compaction strategy options.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.",
            ConfigOption.Type.FIXED,
            String[].class);

    ConfigOption<Boolean> CF_COMPACT_STORAGE = new ConfigOption<>(
            CQL_NS,
            "compact-storage",
            "Whether the storage backend should use compact storage on tables. This option is only available for Cassandra 2 and earlier and defaults to true.",
            ConfigOption.Type.FIXED,
            Boolean.class,
            true);

    // Compression
    ConfigOption<Boolean> CF_COMPRESSION = new ConfigOption<>(
            CQL_NS,
            "compression",
            "Whether the storage backend should use compression when storing the data",
            ConfigOption.Type.FIXED,
            true);

    ConfigOption<String> CF_COMPRESSION_TYPE = new ConfigOption<>(
            CQL_NS,
            "compression-type",
            "The sstable_compression value JanusGraph uses when creating column families. " +
                    "This accepts any value allowed by Cassandra's sstable_compression option. " +
                    "Leave this unset to disable sstable_compression on JanusGraph-created CFs.",
            ConfigOption.Type.MASKABLE,
            "LZ4Compressor");

    ConfigOption<Integer> CF_COMPRESSION_BLOCK_SIZE = new ConfigOption<>(
            CQL_NS,
            "compression-block-size",
            "The size of the compression blocks in kilobytes",
            ConfigOption.Type.FIXED,
            64);

    ConfigOption<Integer> LOCAL_MAX_CONNECTIONS_PER_HOST = new ConfigOption<>(
            CQL_NS,
            "local-max-connections-per-host",
            "The maximum number of connections that can be created per host for local datacenter",
            ConfigOption.Type.FIXED,
            1);

    ConfigOption<Integer> REMOTE_MAX_CONNECTIONS_PER_HOST = new ConfigOption<>(
            CQL_NS,
            "remote-max-connections-per-host",
            "The maximum number of connections that can be created per host for remote datacenter",
            ConfigOption.Type.FIXED,
            1);

    ConfigOption<Integer> MAX_REQUESTS_PER_CONNECTION = new ConfigOption<>(
            CQL_NS,
            "max-requests-per-connection",
            "The maximum number of requests that can be executed concurrently on a connection.",
            ConfigOption.Type.FIXED,
            2048);


    // SSL
    ConfigNamespace SSL_NS = new ConfigNamespace(
            CQL_NS,
            "ssl",
            "Configuration options for SSL");

    ConfigNamespace SSL_TRUSTSTORE_NS = new ConfigNamespace(
            SSL_NS,
            "truststore",
            "Configuration options for SSL Truststore.");

    ConfigOption<Boolean> SSL_ENABLED = new ConfigOption<>(
            SSL_NS,
            "enabled",
            "Controls use of the SSL connection to Cassandra",
            ConfigOption.Type.LOCAL,
            false);

    ConfigOption<String> SSL_TRUSTSTORE_LOCATION = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "location",
            "Marks the location of the SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "password",
            "The password to access SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    //Other options

    ConfigOption<String> SESSION_NAME = new ConfigOption<>(
            CQL_NS,
            "session-name",
            "Default name for the Cassandra session",
            ConfigOption.Type.MASKABLE,
            "JanusGraph Session");

    ConfigOption<String> LOCAL_DATACENTER = new ConfigOption<>(
            CQL_NS,
            "local-datacenter",
            "The name of the local or closest Cassandra datacenter.  When set and not whitespace, " +
                    "this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. " +
                    "When unset or set to whitespace, setLocalDatacenter will not be invoked.",
            /*
             * It's between either LOCAL or MASKABLE. MASKABLE could be useful for cases where all the JanusGraph instances are closest to
             * the same Cassandra DC.
             */
            ConfigOption.Type.MASKABLE,
            String.class,
            "datacenter1");

}

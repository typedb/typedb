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

package grakn.core.graph.graphdb.configuration;

import com.google.common.base.Joiner;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.diskstorage.StandardIndexProvider;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.ConfigNamespace;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.ReadConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.diskstorage.configuration.converter.ReadConfigurationConverter;
import grakn.core.graph.diskstorage.idmanagement.ConflictAvoidanceMode;
import grakn.core.graph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;
import grakn.core.graph.graphdb.configuration.converter.RegisteredAttributeClassesConverter;
import grakn.core.graph.graphdb.database.cache.MetricInstrumentedSchemaCache;
import grakn.core.graph.graphdb.database.cache.SchemaCache;
import grakn.core.graph.graphdb.database.cache.StandardSchemaCache;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.StandardSerializer;
import grakn.core.graph.graphdb.tinkerpop.JanusGraphDefaultSchemaMaker;
import grakn.core.graph.graphdb.transaction.StandardTransactionBuilder;
import grakn.core.graph.graphdb.types.typemaker.DisableDefaultSchemaMaker;
import grakn.core.graph.util.stats.NumberUtil;
import org.apache.commons.configuration.BaseConfiguration;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Provides functionality to configure a {@link JanusGraph} INSTANCE.
 * <p>
 * <p>
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases.
 * <p>
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 */
public class GraphDatabaseConfiguration {

    public static final ConfigNamespace ROOT_NS = new ConfigNamespace(null, "root", "Root Configuration Namespace for the JanusGraph Graph Database");

    // ########## Graph-level Config Options ##########
    // ################################################

    public static final ConfigNamespace GRAPH_NS = new ConfigNamespace(ROOT_NS, "graph",
            "General configuration options");

    public static final ConfigOption<TimestampProviders> TIMESTAMP_PROVIDER = new ConfigOption<>(GRAPH_NS, "timestamps",
            "The timestamp resolution to use when writing to storage and indices. Sets the time granularity for the " +
                    "entire graph cluster. To avoid potential inaccuracies, the configured time resolution should match " +
                    "those of the backend systems. Some JanusGraph storage backends declare a preferred timestamp resolution that " +
                    "reflects design constraints in the underlying service. When the backend provides " +
                    "a preferred default, and when this setting is not explicitly declared in the config file, the backend " +
                    "default is used and the general default associated with this setting is ignored.  An explicit " +
                    "declaration of this setting overrides both the general and backend-specific defaults.",
            ConfigOption.Type.FIXED, TimestampProviders.class, TimestampProviders.MICRO);

    public static final ConfigOption<String> UNIQUE_INSTANCE_ID = new ConfigOption<>(GRAPH_NS, "unique-instance-id",
            "Unique identifier for this JanusGraph instance.  This must be unique among all instances " +
                    "concurrently accessing the same stores or indexes.  It's automatically generated by " +
                    "concatenating the hostname, process id, and a static (process-wide) counter. " +
                    "Leaving it unset is recommended.",
            ConfigOption.Type.LOCAL, String.class);


    public static final ConfigOption<Short> UNIQUE_INSTANCE_ID_SUFFIX = new ConfigOption<>(GRAPH_NS, "unique-instance-id-suffix",
            "When this is set and " + UNIQUE_INSTANCE_ID.getName() + " is not, this JanusGraph " +
                    "instance's unique identifier is generated by concatenating the hex encoded hostname to the " +
                    "provided number.",
            ConfigOption.Type.LOCAL, Short.class);

    public static final ConfigOption<String> INITIAL_JANUSGRAPH_VERSION = new ConfigOption<>(GRAPH_NS, "janusgraph-version",
            "The version of JanusGraph with which this database was created. Automatically set on first start. Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class).hide();

    public static final ConfigOption<String> INITIAL_STORAGE_VERSION = new ConfigOption<>(GRAPH_NS, "storage-version",
            "The version of JanusGraph storage schema with which this database was created. Automatically set on first start of graph. " +
                    "Should only ever be changed if upgraing to a new major release version of JanusGraph that contains schema changes",
            ConfigOption.Type.FIXED, String.class);

    public static ConfigOption<Boolean> ALLOW_UPGRADE = new ConfigOption<>(GRAPH_NS, "allow-upgrade",
            "Setting this to true will allow certain fixed values to be updated such as storage-version. This should only be used for upgrading.",
            ConfigOption.Type.MASKABLE, Boolean.class, false);

    public static final ConfigOption<Boolean> UNIQUE_INSTANCE_ID_HOSTNAME = new ConfigOption<Boolean>(GRAPH_NS, "use-hostname-for-unique-instance-id",
            "When this is set, this JanusGraph's unique instance identifier is set to the hostname. If " + UNIQUE_INSTANCE_ID_SUFFIX.getName() +
                    " is also set, then the identifier is set to <hostname><suffix>.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<Boolean> REPLACE_INSTANCE_IF_EXISTS = new ConfigOption<Boolean>(GRAPH_NS, "replace-instance-if-exists",
            "If a JanusGraph instance with the same instance identifier already exists, the usage of this " +
                    "configuration option results in the opening of this graph anwyay.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<Boolean> ALLOW_STALE_CONFIG = new ConfigOption<>(GRAPH_NS, "allow-stale-config",
            "Whether to allow the local and storage-backend-hosted copies of the configuration to contain conflicting values for " +
                    "options with any of the following types: " + Joiner.on(", ").join(ConfigOption.getManagedTypes()) + ".  " +
                    "These types are managed globally through the storage backend and cannot be overridden by changing the " +
                    "local configuration.  This type of conflict usually indicates misconfiguration.  When this option is true, " +
                    "JanusGraph will LOG these option conflicts, but continue normal operation using the storage-backend-hosted value " +
                    "for each conflicted option.  When this option is false, JanusGraph will LOG these option conflicts, but then it " +
                    "will throw an exception, refusing to start.",
            ConfigOption.Type.MASKABLE, Boolean.class, true);

    // ################ INSTANCE REGISTRATION (system) #######################
    // ##############################################################

    public static final ConfigNamespace REGISTRATION_NS = new ConfigNamespace(ROOT_NS, "system-registration",
            "This is used internally to keep track of open instances.", true);

    public static final ConfigOption<Instant> REGISTRATION_TIME = new ConfigOption<>(REGISTRATION_NS, "startup-time",
            "Timestamp when this instance was started.  Automatically set.", ConfigOption.Type.GLOBAL, Instant.class).hide();


    // ########## OLAP Style Processing ##########
    // ################################################

    public static final ConfigNamespace JOB_NS = new ConfigNamespace(null, "job", "Root Configuration Namespace for JanusGraph OLAP jobs");

    public static final ConfigOption<Long> JOB_START_TIME = new ConfigOption<>(JOB_NS, "start-time",
            "Timestamp (ms since epoch) when the job started. Automatically set.", ConfigOption.Type.LOCAL, Long.class).hide();


    public static final ConfigNamespace COMPUTER_NS = new ConfigNamespace(ROOT_NS, "computer",
            "GraphComputer related configuration");

    public static final ConfigOption<String> COMPUTER_RESULT_MODE = new ConfigOption<>(COMPUTER_NS, "result-mode",
            "How the graph computer should return the computed results. 'persist' for writing them into the graph, " +
                    "'localtx' for writing them into the local transaction, or 'none' (default)", ConfigOption.Type.MASKABLE, "none");


    // ################ Transaction #######################
    // ################################################

    public static final ConfigNamespace TRANSACTION_NS = new ConfigNamespace(ROOT_NS, "tx",
            "Configuration options for transaction handling");

    public static final ConfigOption<Boolean> SYSTEM_LOG_TRANSACTIONS = new ConfigOption<>(TRANSACTION_NS, "LOG-tx",
            "Whether transaction mutations should be logged to JanusGraph's write-ahead transaction LOG which can be used for recovery of partially failed transactions",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Duration> MAX_COMMIT_TIME = new ConfigOption<>(TRANSACTION_NS, "max-commit-time",
            "Maximum time (in ms) that a transaction might take to commit against all backends. This is used by the distributed " +
                    "write-ahead LOG processing to determine when a transaction can be considered failed (i.e. after this time has elapsed)." +
                    "Must be longer than the maximum allowed write time.",
            ConfigOption.Type.GLOBAL, Duration.ofSeconds(10));


    public static final ConfigNamespace TRANSACTION_RECOVERY_NS = new ConfigNamespace(TRANSACTION_NS, "recovery",
            "Configuration options for transaction recovery processes");

    public static final ConfigOption<Boolean> VERBOSE_TX_RECOVERY = new ConfigOption<>(TRANSACTION_RECOVERY_NS, "verbose",
            "Whether the transaction recovery system should print recovered transactions and other activity to standard output",
            ConfigOption.Type.MASKABLE, false);

    // ################ Query Processing #######################
    // ################################################

    public static final ConfigNamespace QUERY_NS = new ConfigNamespace(ROOT_NS, "query",
            "Configuration options for query processing");

    public static final ConfigOption<Boolean> IGNORE_UNKNOWN_INDEX_FIELD = new ConfigOption<>(QUERY_NS, "ignore-unknown-index-key",
            "Whether to ignore undefined types encountered in user-provided index queries",
            ConfigOption.Type.MASKABLE, false);

    public static final String UNKNOWN_FIELD_NAME = "unknown_key";


    public static final ConfigOption<Boolean> FORCE_INDEX_USAGE = new ConfigOption<>(QUERY_NS, "force-index",
            "Whether JanusGraph should throw an exception if a graph query cannot be answered using an index. Doing so" +
                    "limits the functionality of JanusGraph's graph queries but ensures that slow graph queries are avoided " +
                    "on large graphs. Recommended for production use of JanusGraph.",
            ConfigOption.Type.MASKABLE, false);


    public static final ConfigOption<Boolean> PROPERTY_PREFETCHING = new ConfigOption<>(QUERY_NS, "fast-property",
            "Whether to pre-fetch all properties on first singular vertex property access. This can eliminate backend calls on subsequent" +
                    "property access for the same vertex at the expense of retrieving all properties at once. This can be " +
                    "expensive for vertices with many properties",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> ADJUST_LIMIT = new ConfigOption<>(QUERY_NS, "smart-limit",
            "Whether the query optimizer should try to guess a smart limit for the query to ensure responsiveness in " +
                    "light of possibly large result sets. Those will be loaded incrementally if this option is enabled.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> USE_MULTIQUERY = new ConfigOption<>(QUERY_NS, "batch",
            "Whether traversal queries should be batched when executed against the storage backend. This can lead to significant " +
                    "performance improvement if there is a non-trivial latency to the backend.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Boolean> BATCH_PROPERTY_PREFETCHING = new ConfigOption<>(QUERY_NS, "batch-property-prefetch",
            "Whether to do a batched pre-fetch of all properties on adjacent vertices against the storage backend prior to evaluating a has condition against those vertices. " +
                    "Because these vertex properties will be loaded into the transaction-level cache of recently-used vertices when the condition is evaluated this can " +
                    "lead to significant performance improvement if there are many edges to adjacent vertices and there is a non-trivial latency to the backend.",
            ConfigOption.Type.MASKABLE, false);

    // ################ CACHE #######################
    // ################################################

    public static final ConfigNamespace CACHE_NS = new ConfigNamespace(ROOT_NS, "cache", "Configuration options that modify JanusGraph's caching behavior");

    public static final ConfigOption<Boolean> DB_CACHE = new ConfigOption<>(CACHE_NS, "db-cache",
            "Whether to enable JanusGraph's database-level cache, which is shared across all transactions. " +
                    "Enabling this option speeds up traversals by holding hot graph elements in memory, " +
                    "but also increases the likelihood of reading stale data.  Disabling it forces each " +
                    "transaction to independently fetch graph elements from storage before reading/writing them.",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The size of the database level cache.
     * If this value is between 0.0 (strictly bigger) and 1.0 (strictly smaller), then it is interpreted as a
     * percentage of the total heap space available to the JVM this JanusGraph instance is running in.
     * If this value is bigger than 1.0 it is interpreted as an absolute size in bytes.
     */
    public static final ConfigOption<Double> DB_CACHE_SIZE = new ConfigOption<>(CACHE_NS, "db-cache-size",
            "Size of JanusGraph's database level cache.  Values between 0 and 1 are interpreted as a percentage " +
                    "of VM heap, while larger values are interpreted as an absolute size in bytes.",
            ConfigOption.Type.MASKABLE, 0.3);

    /**
     * How long the database level cache will keep keys expired while the mutations that triggered the expiration
     * are being persisted. This value should be larger than the time it takes for persisted mutations to become visible.
     * This setting only ever makes sense for distributed storage backends where writes may be accepted but are not
     * immediately readable.
     */
    public static final ConfigOption<Integer> DB_CACHE_CLEAN_WAIT = new ConfigOption<>(CACHE_NS, "db-cache-clean-wait",
            "How long, in milliseconds, database-level cache will keep entries after flushing them.  " +
                    "This option is only useful on distributed storage backends that are capable of acknowledging writes " +
                    "without necessarily making them immediately visible.",
            ConfigOption.Type.GLOBAL_OFFLINE, 50);

    /**
     * The default expiration time for elements held in the database level cache. This is the time period before
     * JanusGraph will check against storage backend for a newer query answer.
     * Setting this value to 0 will cache elements forever (unless they get evicted due to space constraints). This only
     * makes sense when this is the only JanusGraph instance interacting with a storage backend.
     */
    public static final ConfigOption<Long> DB_CACHE_TIME = new ConfigOption<>(CACHE_NS, "db-cache-time",
            "Default expiration time, in milliseconds, for entries in the database-level cache. " +
                    "Entries are evicted when they reach this age even if the cache has room to spare. " +
                    "Set to 0 to disable expiration (cache entries live forever or until memory pressure " +
                    "triggers eviction when set to 0).",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000L);

    /**
     * Configures the maximum number of recently-used vertices cached by a transaction. The smaller the cache size, the
     * less memory a transaction can consume at maximum. For many concurrent, long running transactions in memory constraint
     * environments, reducing the cache size can avoid OutOfMemory and GC limit exceeded exceptions.
     * Note, however, that all modifications in a transaction must always be kept in memory and hence this setting does not
     * have much impact on write intense transactions. Those must be split into smaller transactions in the case of memory errors.
     * <p>
     * The recently-used vertex cache can contain both dirty and clean vertices, that is, both vertices which have been
     * created or updated in the current transaction and vertices which have only been read in the current transaction.
     */
    public static final ConfigOption<Integer> TX_CACHE_SIZE = new ConfigOption<>(CACHE_NS, "tx-cache-size",
            "Maximum size of the transaction-level cache of recently-used vertices.",
            ConfigOption.Type.MASKABLE, 20000);

    /**
     * Configures the initial size of the dirty (modified) vertex map used by a transaction.  All vertices created or
     * updated by a transaction are held in that transaction's dirty vertex map until the transaction commits.
     * This option sets the initial size of the dirty map.  Unlike {@link #TX_CACHE_SIZE}, this is not a maximum.
     * The transaction will transparently allocate more space to store dirty vertices if this initial size hint
     * is exceeded.  Transactions that know how many vertices they are likely to modify a priori can avoid resize
     * costs associated with growing the dirty vertex data structure by setting this option.
     */
    public static final ConfigOption<Integer> TX_DIRTY_SIZE = new ConfigOption<>(CACHE_NS, "tx-dirty-size",
            "Initial size of the transaction-level cache of uncommitted dirty vertices. " +
                    "This is a performance hint for write-heavy, performance-sensitive transactional workloads. " +
                    "If set, it should roughly match the median vertices modified per transaction.",
            ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is disabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH = 32;

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is enabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITH_BATCH = 4096;


    // ################ STORAGE #######################
    // ################################################

    public static final ConfigNamespace STORAGE_NS = new ConfigNamespace(ROOT_NS, "storage", "Configuration options for the storage backend.  Some options are applicable only for certain backends.");

    /**
     * Storage root directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_ROOT = new ConfigOption<>(STORAGE_NS, "root",
            "Storage root directory for those storage backends that require local storage. " +
                    "If you do not supply storage.directory and you do supply graph.graphname, then your data " +
                    "will be stored in the directory equivalent to <STORAGE_ROOT>/<GRAPH_NAME>.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_DIRECTORY = new ConfigOption<>(STORAGE_NS, "directory",
            "Storage directory for those storage backends that require local storage.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Path to a configuration file for those storage backends that
     * require/support a separate config file
     */
    public static final ConfigOption<String> STORAGE_CONF_FILE = new ConfigOption<>(STORAGE_NS, "conf-file",
            "Path to a configuration file for those storage backends which require/support a single separate config file.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_BACKEND = new ConfigOption<>(STORAGE_NS, "backend",
            "The primary persistence provider used by JanusGraph.  This is required.  It should be set one of " +
                    "JanusGraph's built-in shorthand names for its standard storage backends " +
                    "(shorthands: inmemory, cql) " +
                    "or to the full package and classname of a custom/third-party StoreManager implementation.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final ConfigOption<Boolean> STORAGE_BATCH = new ConfigOption<>(STORAGE_NS, "batch-loading",
            "Whether to enable batch loading into the storage backend",
            ConfigOption.Type.LOCAL, false);

    /**
     * Enables transactions on storage backends that support them
     */
    public static final ConfigOption<Boolean> STORAGE_TRANSACTIONAL = new ConfigOption<>(STORAGE_NS, "transactions",
            "Enables transactions on storage backends that support them",
            ConfigOption.Type.MASKABLE, true);

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final ConfigOption<Integer> BUFFER_SIZE = new ConfigOption<>(STORAGE_NS, "buffer-size",
            "Size of the batch in which mutations are persisted",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> STORAGE_WRITE_WAITTIME = new ConfigOption<>(STORAGE_NS, "write-time",
            "Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operation" +
                    "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(100L));

    public static final ConfigOption<Duration> STORAGE_READ_WAITTIME = new ConfigOption<>(STORAGE_NS, "read-time",
            "Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operation" +
                    "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(10L));

    /**
     * If enabled, JanusGraph attempts to parallelize storage operations against the storage backend using a fixed thread pool shared
     * across the entire JanusGraph graph database instance. Parallelization is only applicable to certain storage operations and
     * can be beneficial when the operation is I/O bound.
     */
    public static final ConfigOption<Boolean> PARALLEL_BACKEND_OPS = new ConfigOption<>(STORAGE_NS, "parallel-backend-ops",
            "Whether JanusGraph should attempt to parallelize storage operations",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String[]> STORAGE_HOSTS = new ConfigOption<>(STORAGE_NS, "hostname",
            "The hostname or comma-separated list of hostnames of storage backend servers.  " +
                    "This is only applicable to some storage backends, such as cassandra and hbase.",
            ConfigOption.Type.LOCAL, new String[]{InetAddress.getLoopbackAddress().getHostAddress()});

    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     */
    public static final ConfigOption<Integer> STORAGE_PORT = new ConfigOption<>(STORAGE_NS, "port",
            "The port on which to connect to storage backend servers. For HBase, it is the Zookeeper port.",
            ConfigOption.Type.LOCAL, Integer.class);

    /**
     * Username and password keys to be used to specify an access credential that may be needed to connect
     * with a secured storage backend.
     */
    public static final ConfigOption<String> AUTH_USERNAME = new ConfigOption<>(STORAGE_NS, "username",
            "Username to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> AUTH_PASSWORD = new ConfigOption<>(STORAGE_NS, "password",
            "Password to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Default timeout when connecting to a remote database instance
     * <p>
     */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = new ConfigOption<>(STORAGE_NS, "connection-timeout",
            "Default timeout, in milliseconds, when connecting to a remote database instance",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(10000L));

    /**
     * Time in milliseconds for backend manager to wait for the storage backends to
     * become available when JanusGraph is run in server mode. Should the backend manager
     * experience exceptions when attempting to access the storage backend it will retry
     * until this timeout is exceeded.
     * <p>
     * A wait time of 0 disables waiting.
     * <p>
     */
    public static final ConfigOption<Duration> SETUP_WAITTIME = new ConfigOption<>(STORAGE_NS, "setup-wait",
            "Time in milliseconds for backend manager to wait for the storage backends to become available when JanusGraph is run in server mode",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(60000L));

    /**
     * Default number of results to pull over the wire when iterating over a distributed
     * storage backend.
     * This is batch size of results to pull when iterating a result set.
     */
    public static final ConfigOption<Integer> PAGE_SIZE = new ConfigOption<>(STORAGE_NS, "page-size",
            "JanusGraph break requests that may return many results from distributed storage backends " +
                    "into a series of requests for small chunks/pages of results, where each chunk contains " +
                    "up to this many elements.",
            ConfigOption.Type.MASKABLE, 5000);

    public static final ConfigOption<Boolean> DROP_ON_CLEAR = new ConfigOption<>(STORAGE_NS, "drop-on-clear",
            "Whether to drop the graph database (true) or delete rows (false) when clearing storage. " +
                    "Note that some backends always drop the graph database when clearing storage. Also note that indices are " +
                    "always dropped when clearing storage.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace LOCK_NS = new ConfigNamespace(STORAGE_NS, "lock", "Options for locking on eventually-consistent stores");

    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final ConfigOption<Integer> LOCK_RETRY = new ConfigOption<>(LOCK_NS, "retries",
            "Number of times the system attempts to acquire a lock before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 3);

    /**
     * The number of milliseconds the system waits for a lock application to be acknowledged by the storage backend.
     * Also, the time waited at the end of all lock applications before verifying that the applications were successful.
     * This value should be a small multiple of the average consistent write time.
     */
    public static final ConfigOption<Duration> LOCK_WAIT = new ConfigOption<>(LOCK_NS, "wait-time",
            "Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend. " +
                    "Also, the time waited at the end of all lock applications before verifying that the applications were successful. " +
                    "This value should be a small multiple of the average consistent write time.",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(100L));

    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final ConfigOption<Duration> LOCK_EXPIRE = new ConfigOption<>(LOCK_NS, "expiry-time",
            "Number of milliseconds after which a lock is considered to have expired. " +
                    "Lock applications that were not released are considered expired after this time and released. " +
                    "This value should be larger than the maximum time a transaction can take in order to guarantee " +
                    "that no correctly held applications are expired pre-maturely and as small as possible to avoid dead lock.",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300 * 1000L));

    /**
     * Whether to attempt to delete expired locks from the storage backend. True
     * will attempt to delete expired locks in a background daemon thread. False
     * will never attempt to delete expired locks. This option is only
     * meaningful for the default lock backend.
     */
    public static final ConfigOption<Boolean> LOCK_CLEAN_EXPIRED = new ConfigOption<>(LOCK_NS, "clean-expired",
            "Whether to delete expired locks from the storage backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * Configuration setting key for the local lock mediator prefix
     */
    public static final ConfigOption<String> LOCK_LOCAL_MEDIATOR_GROUP =
            new ConfigOption<>(LOCK_NS, "local-mediator-group",
                    "This option determines the LocalLockMediator instance used for early detection of lock contention " +
                            "between concurrent JanusGraph graph instances within the same process which are connected to the same " +
                            "storage backend.  JanusGraph instances that have the same value for this variable will attempt to discover " +
                            "lock contention among themselves in memory before proceeding with the general-case distributed locking " +
                            "code.  JanusGraph generates an appropriate default value for this option at startup.  Overridding " +
                            "the default is generally only useful in testing.", ConfigOption.Type.LOCAL, String.class);


    // ################ STORAGE - META #######################

    public static final ConfigNamespace STORE_META_NS = new ConfigNamespace(STORAGE_NS, "meta", "Meta data to include in storage backend retrievals", true);

    public static final ConfigOption<Boolean> STORE_META_TIMESTAMPS = new ConfigOption<>(STORE_META_NS, "timestamps",
            "Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_TTL = new ConfigOption<>(STORE_META_NS, "ttl",
            "Whether to include ttl in retrieved entries for storage backends that support storage and retrieval of cell level TTL",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_VISIBILITY = new ConfigOption<>(STORE_META_NS, "visibility",
            "Whether to include visibility in retrieved entries for storage backends that support cell level visibility",
            ConfigOption.Type.GLOBAL, true);


    // ################ CLUSTERING ###########################
    // ################################################

    public static final ConfigNamespace CLUSTER_NS = new ConfigNamespace(ROOT_NS, "cluster", "Configuration options for multi-machine deployments");

    public static final ConfigOption<Integer> CLUSTER_MAX_PARTITIONS = new ConfigOption<>(CLUSTER_NS, "max-partitions",
            "The number of virtual partition blocks created in the partitioned graph. This should be larger than the maximum expected number of nodes" +
                    " in the JanusGraph graph cluster. Must be greater than 1 and a power of 2.",
            ConfigOption.Type.FIXED, 32, integer -> integer != null && integer > 1 && NumberUtil.isPowerOf2(integer));


    // ################ IDS ###########################
    // ################################################

    public static final ConfigNamespace IDS_NS = new ConfigNamespace(ROOT_NS, "ids", "General configuration options for graph element IDs");

    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final ConfigOption<Integer> IDS_BLOCK_SIZE = new ConfigOption<>(IDS_NS, "block-size",
            "Globally reserve graph element IDs in chunks of this size.  Setting this too low will make commits " +
                    "frequently block on slow reservation requests.  Setting it too high will result in IDs wasted when a " +
                    "graph instance shuts down with reserved but mostly-unused blocks.",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000);

    /**
     * The name of the ID store. Currently this defaults to janusgraph_ids. You can override the ID store to
     * facilitate migration from JanusGraph's predecessor, Titan. Previously, this KCVStore was named titan_ids.
     */
    public static final ConfigOption<String> IDS_STORE_NAME = new ConfigOption<>(IDS_NS, "store-name",
            "The name of the ID KCVStore. IDS_STORE_NAME is meant to be used only for backward compatibility with Titan, " +
                    "and should not be used explicitly in normal operations or in new graphs.",
            ConfigOption.Type.GLOBAL_OFFLINE, JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME);


    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final ConfigOption<Boolean> IDS_FLUSH = new ConfigOption<>(IDS_NS, "flush",
            "When true, vertices and edges are assigned IDs immediately upon creation.  When false, " +
                    "IDs are assigned only when the transaction commits.",
            ConfigOption.Type.MASKABLE, true);

    /**
     * The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block
     * of ids. Note, that failure to allocate a new id block will cause the entire database to fail, hence this value
     * should be set conservatively. Choose a high value if there is a lot of contention around id allocation.
     */
    public static final ConfigOption<Duration> IDS_RENEW_TIMEOUT = new ConfigOption<>(IDS_NS, "renew-timeout",
            "The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block of ids",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(120000L));

    /**
     * Configures when the id pool manager will attempt to allocate a new id block. When all but the configured percentage
     * of the current block is consumed, a new block will be allocated. Larger values should be used if a lot of ids
     * are allocated in a short amount of time. Value must be in (0,1].
     */
    public static final ConfigOption<Double> IDS_RENEW_BUFFER_PERCENTAGE = new ConfigOption<>(IDS_NS, "renew-percentage",
            "When the most-recently-reserved ID block has only this percentage of its total IDs remaining " +
                    "(expressed as a value between 0 and 1), JanusGraph asynchronously begins reserving another block. " +
                    "This helps avoid transaction commits waiting on ID reservation even if the block size is relatively small.",
            ConfigOption.Type.MASKABLE, 0.3);

    // ################ IDAUTHORITY ###################
    // ################################################

    public static final ConfigNamespace IDAUTHORITY_NS = new ConfigNamespace(IDS_NS, "authority", "Configuration options for graph element ID reservation/allocation");

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final ConfigOption<Duration> IDAUTHORITY_WAIT = new ConfigOption<>(IDAUTHORITY_NS, "wait-time",
            "The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300L));

    /**
     * Sets the strategy used by {@link ConsistentKeyIDAuthority} to avoid
     * contention in ID block allocation between JanusGraph instances concurrently
     * sharing a single distributed storage backend.
     */
    // This is set to GLOBAL_OFFLINE as opposed to MASKABLE or GLOBAL to prevent mixing both global-randomized and local-manual modes within the same cluster
    public static final ConfigOption<ConflictAvoidanceMode> IDAUTHORITY_CONFLICT_AVOIDANCE = new ConfigOption<>(IDAUTHORITY_NS, "conflict-avoidance-mode",
            "This setting helps separate JanusGraph instances sharing a single graph storage backend avoid contention when reserving ID blocks, " +
                    "increasing overall throughput.",
            ConfigOption.Type.GLOBAL_OFFLINE, ConflictAvoidanceMode.class, ConflictAvoidanceMode.NONE);

    /**
     * When JanusGraph allocates IDs with {@link ConflictAvoidanceMode#GLOBAL_AUTO}
     * configured, it picks a random unique ID marker and attempts to allocate IDs
     * from a partition using the marker. The ID markers function as
     * subpartitions with each ID partition. If the attempt fails because that
     * partition + unique id combination is already completely allocated, then
     * JanusGraph will generate a new random unique ID and try again. This controls
     * the maximum number of attempts before JanusGraph assumes the entire partition
     * is allocated and fails the request. It must be set to at least 1 and
     * should generally be set to 3 or more.
     * <p>
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is not configured to
     * {@link ConflictAvoidanceMode#GLOBAL_AUTO}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_RETRIES = new ConfigOption<>(IDAUTHORITY_NS, "randomized-conflict-avoidance-retries",
            "Number of times the system attempts ID block reservations with random conflict avoidance tags before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 5);

    /**
     * Configures the number of bits of JanusGraph assigned ids that are reserved for a unique id marker that
     * allows the id allocation to be scaled over multiple sub-clusters and to reduce race-conditions
     * when a lot of JanusGraph instances attempt to allocate ids at the same time (e.g. during parallel bulk loading)
     * <p>
     * IMPORTANT: This should never ever, ever be modified from its initial value and ALL JanusGraph instances must use the
     * same value. Otherwise, data corruption will occur.
     * <p>
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link ConflictAvoidanceMode#NONE}. However, note that while the
     * conflict avoidance mode can be changed, this setting cannot ever be changed and must therefore be considered a priori.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_BITS = new ConfigOption<>(IDAUTHORITY_NS, "conflict-avoidance-tag-bits",
            "Configures the number of bits of JanusGraph-assigned element IDs that are reserved for the conflict avoidance tag",
            ConfigOption.Type.FIXED, 4, uniqueIdBitWidth -> uniqueIdBitWidth >= 0 && uniqueIdBitWidth <= 16);

    /**
     * Unique id marker to be used by this JanusGraph instance when allocating ids. The unique id marker
     * must be non-negative and fit within the number of unique id bits configured.
     * By assigning different unique id markers to individual JanusGraph instances it can be assured
     * that those instances don't conflict with one another when attempting to allocate new id blocks.
     * <p>
     * IMPORTANT: The configured unique id marker must fit within the configured unique id bit width.
     * <p>
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link ConflictAvoidanceMode#NONE}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_TAG = new ConfigOption<>(IDAUTHORITY_NS, "conflict-avoidance-tag",
            "Conflict avoidance tag to be used by this JanusGraph instance when allocating IDs",
            ConfigOption.Type.LOCAL, 0);


    // ############## External Index ######################
    // ################################################

    public static final ConfigNamespace INDEX_NS = new ConfigNamespace(ROOT_NS, "index", "Configuration options for the individual indexing backends", true);


    /**
     * Define the indexing backed to use for index support
     */
    public static final ConfigOption<String> INDEX_BACKEND = new ConfigOption<>(INDEX_NS, "backend",
            "The indexing backend used to extend and optimize JanusGraph's query functionality. " +
                    "This setting is optional.  JanusGraph can use multiple heterogeneous index backends.  " +
                    "Hence, this option can appear more than once, so long as the user-defined name between " +
                    "\"" + INDEX_NS.getName() + "\" and \"backend\" is unique among appearances." +
                    "Similar to the storage backend, this should be set to one of " +
                    "JanusGraph's built-in shorthand names for its standard index backends " +
                    "(shorthands: " + Joiner.on(", ").join(StandardIndexProvider.getAllShorthands()) + ") " +
                    "or to the full package and classname of a custom/third-party IndexProvider implementation.",
            ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

    public static final ConfigOption<String> INDEX_DIRECTORY = new ConfigOption<>(INDEX_NS, "directory",
            "Directory to store index data locally",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<>(INDEX_NS, "index-name",
            "Name of the index if required by the indexing backend",
            ConfigOption.Type.GLOBAL_OFFLINE, "janusgraph");

    public static final ConfigOption<String[]> INDEX_HOSTS = new ConfigOption<>(INDEX_NS, "hostname",
            "The hostname or comma-separated list of hostnames of index backend servers.  " +
                    "This is only applicable to some index backends, such as elasticsearch and solr.",
            ConfigOption.Type.MASKABLE, new String[]{InetAddress.getLoopbackAddress().getHostAddress()});

    public static final ConfigOption<Integer> INDEX_PORT = new ConfigOption<>(INDEX_NS, "port",
            "The port on which to connect to index backend servers",
            ConfigOption.Type.MASKABLE, Integer.class);

    public static final ConfigOption<String> INDEX_CONF_FILE = new ConfigOption<>(INDEX_NS, "conf-file",
            "Path to a configuration file for those indexing backends that require/support a separate config file",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<Integer> INDEX_MAX_RESULT_SET_SIZE = new ConfigOption<>(INDEX_NS, "max-result-set-size",
            "Maximum number of results to return if no limit is specified. For index backends that support scrolling, it represents " +
                    "the number of results in each batch",
            ConfigOption.Type.MASKABLE, 50);

    public static final ConfigOption<Boolean> INDEX_NAME_MAPPING = new ConfigOption<>(INDEX_NS, "map-name",
            "Whether to use the name of the property key as the field name in the index. It must be ensured, that the" +
                    "indexed property key names are valid field names. Renaming the property key will NOT rename the field " +
                    "and its the developers responsibility to avoid field collisions.",
            ConfigOption.Type.GLOBAL, true);


    // ############## Logging System ######################
    // ################################################

    public static final ConfigNamespace LOG_NS = new ConfigNamespace(ROOT_NS, "log", "Configuration options for JanusGraph's logging system", true);

    public static final String MANAGEMENT_LOG = "janusgraph";
    public static final String TRANSACTION_LOG = "tx";
    public static final String USER_LOG = "user";
    public static final String USER_LOG_PREFIX = "ulog_";

    public static final Duration TRANSACTION_LOG_DEFAULT_TTL = Duration.ofDays(7);

    public static final ConfigOption<String> LOG_BACKEND = new ConfigOption<>(LOG_NS, "backend",
            "Define the LOG backed to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "default");

    public static final ConfigOption<Integer> LOG_NUM_BUCKETS = new ConfigOption<>(LOG_NS, "num-buckets",
            "The number of buckets to split LOG entries into for load balancing",
            ConfigOption.Type.GLOBAL_OFFLINE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_SEND_BATCH_SIZE = new ConfigOption<>(LOG_NS, "send-batch-size",
            "Maximum number of LOG messages to batch up for sending for logging implementations that support batch sending",
            ConfigOption.Type.MASKABLE, 256, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_READ_BATCH_SIZE = new ConfigOption<>(LOG_NS, "read-batch-size",
            "Maximum number of LOG messages to read at a time for logging implementations that read messages in batches",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_SEND_DELAY = new ConfigOption<>(LOG_NS, "send-delay",
            "Maximum time in ms that messages can be buffered locally before sending in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(1000L));

    public static final ConfigOption<Duration> LOG_READ_INTERVAL = new ConfigOption<>(LOG_NS, "read-interval",
            "Time in ms between message readings from the backend for this logging implementations that read message in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(5000L));

    public static final ConfigOption<Integer> LOG_READ_THREADS = new ConfigOption<>(LOG_NS, "read-threads",
            "Number of threads to be used in reading and processing LOG messages",
            ConfigOption.Type.MASKABLE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_STORE_TTL = new ConfigOption<Duration>(LOG_NS, "ttl",
            "Sets a TTL on all LOG entries, meaning" +
                    "that all entries added to this LOG expire after the configured amount of time. Requires" +
                    "that the LOG implementation supports TTL.",
            ConfigOption.Type.GLOBAL, Duration.class, sd -> null != sd && !sd.isZero());

    // ############## Attributes ######################
    // ################################################

    public static final ConfigNamespace ATTRIBUTE_NS = new ConfigNamespace(ROOT_NS, "attributes", "Configuration options for attribute handling");

    public static final ConfigNamespace CUSTOM_ATTRIBUTE_NS = new ConfigNamespace(ATTRIBUTE_NS, "custom", "Custom attribute serialization and handling", true);

    public static final String ATTRIBUTE_PREFIX = "attribute";

    public static final ConfigOption<String> CUSTOM_ATTRIBUTE_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS, "attribute-class",
            "Class of the custom attribute to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);
    public static final ConfigOption<String> CUSTOM_SERIALIZER_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS, "serializer-class",
            "Class of the custom attribute serializer to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);

    // ################ Metrics #######################
    // ################################################

    /**
     * Configuration key prefix for Metrics.
     */
    public static final ConfigNamespace METRICS_NS = new ConfigNamespace(ROOT_NS, "metrics", "Configuration options for metrics reporting");

    /**
     * Whether to enable basic timing and operation count monitoring on backend
     * methods using the {@code com.codahale.metrics} package.
     */
    public static final ConfigOption<Boolean> BASIC_METRICS = new ConfigOption<>(METRICS_NS, "enabled",
            "Whether to enable basic timing and operation count monitoring on backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * This is the prefix used outside of a graph database configuration, or for
     * operations where a system-internal transaction is necessary as an
     * implementation detail. It currently can't be modified, though there is no
     * substantial technical obstacle preventing it from being configured --
     * some kind of configuration object is in scope everywhere it is used, and
     * it could theoretically be stored in and read from that object.
     */
    public static final String METRICS_PREFIX_DEFAULT = "grakn.core.graph";
    public static final String METRICS_SYSTEM_PREFIX_DEFAULT = METRICS_PREFIX_DEFAULT + "." + "sys";
    public static final String METRICS_SCHEMA_PREFIX_DEFAULT = METRICS_SYSTEM_PREFIX_DEFAULT + "." + "schema";

    /**
     * The default name prefix for Metrics reported by JanusGraph. All metric names
     * will begin with this string and a period. This value can be overridden on
     * a transaction-specific basis through
     * {@link StandardTransactionBuilder#groupName(String)}.
     * <p>
     * Default = {@literal #METRICS_PREFIX_DEFAULT}
     */
    public static final ConfigOption<String> METRICS_PREFIX = new ConfigOption<>(METRICS_NS, "prefix",
            "The default name prefix for Metrics reported by JanusGraph.",
            ConfigOption.Type.MASKABLE, METRICS_PREFIX_DEFAULT);

    /**
     * Whether to aggregate measurements for the edge store, vertex index, edge
     * index, and ID store.
     * <p>
     * If true, then metrics for each of these backends will use the same metric
     * name ("stores"). All of their measurements will be combined. This setting
     * measures the sum of JanusGraph's backend activity without distinguishing
     * between contributions of its various internal stores.
     * <p>
     * If false, then metrics for each of these backends will use a unique
     * metric name ("idStore", "edgeStore", "vertexIndex", and "edgeIndex").
     * This setting exposes the activity associated with each backend component,
     * but it also multiplies the number of measurements involved by four.
     * <p>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final ConfigOption<Boolean> METRICS_MERGE_STORES = new ConfigOption<>(METRICS_NS, "merge-stores",
            "Whether to aggregate measurements for the edge store, vertex index, edge index, and ID store",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace METRICS_CONSOLE_NS = new ConfigNamespace(METRICS_NS, "console", "Configuration options for metrics reporting to console");


    public static final ConfigNamespace METRICS_JMX_NS = new ConfigNamespace(METRICS_NS, "jmx", "Configuration options for metrics reporting through JMX");

    /**
     * The JMX domain in which to report Metrics. If null, then Metrics applies
     * its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_DOMAIN = new ConfigOption<>(METRICS_JMX_NS, "domain",
            "The JMX domain in which to report Metrics",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The configuration namespace within {@link #METRICS_NS} for
     * Graphite.
     */
    public static final ConfigNamespace METRICS_GRAPHITE_NS = new ConfigNamespace(METRICS_NS, "graphite", "Configuration options for metrics reporting through Graphite");

    /**
     * The hostname to receive Graphite plaintext protocol metric data. Setting
     * this config key has no effect unless {@link #GRAPHITE_INTERVAL} is also
     * set.
     */
    public static final ConfigOption<String> GRAPHITE_HOST = new ConfigOption<>(METRICS_GRAPHITE_NS, "hostname",
            "The hostname to receive Graphite plaintext protocol metric data",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host specified {@link #GRAPHITE_HOST}. This has no effect unless
     * {@link #GRAPHITE_HOST} is also set.
     */
    public static final ConfigOption<Duration> GRAPHITE_INTERVAL = new ConfigOption<>(METRICS_GRAPHITE_NS, "interval",
            "The number of milliseconds to wait between sending Metrics data",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The port to which Graphite data are sent.
     * <p>
     */
    public static final ConfigOption<Integer> GRAPHITE_PORT = new ConfigOption<>(METRICS_GRAPHITE_NS, "port",
            "The port to which Graphite data are sent",
            ConfigOption.Type.MASKABLE, 2003);

    /**
     * A Graphite-specific prefix for reported metrics. If non-null, Metrics
     * prepends this and a "." to all metric names before reporting them to
     * Graphite.
     * <p>
     */
    public static final ConfigOption<String> GRAPHITE_PREFIX = new ConfigOption<>(METRICS_GRAPHITE_NS, "prefix",
            "A Graphite-specific prefix for reported metrics",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace GREMLIN_NS = new ConfigNamespace(ROOT_NS, "gremlin",
            "Gremlin configuration options");

    // ################ Begin Class Definition #######################
    // ###############################################################

    public static final String SYSTEM_PROPERTIES_STORE_NAME = "system_properties";
    public static final String SYSTEM_CONFIGURATION_IDENTIFIER = "configuration";

    private final Configuration configuration;
    private final ReadConfiguration configurationAtOpen;
    private final String uniqueGraphId;
    private final StoreFeatures storeFeatures;

    private boolean flushIDs;
    private boolean forceIndexUsage;
    private boolean batchLoading;
    private int txVertexCacheSize;
    private int txDirtyVertexSize;
    private DefaultSchemaMaker defaultSchemaMaker;
    private Boolean propertyPrefetching;
    private boolean adjustQueryLimit;
    private Boolean useMultiQuery;
    private Boolean batchPropertyPrefetching;
    private boolean logTransactions;
    private String metricsPrefix;
    private String unknownIndexKeyName;


    public GraphDatabaseConfiguration(ReadConfiguration configurationAtOpen, Configuration configuration, StoreFeatures storeFeatures) {
        this.configurationAtOpen = configurationAtOpen;
        this.uniqueGraphId = configuration.get(UNIQUE_INSTANCE_ID);
        this.configuration = configuration;
        this.storeFeatures = storeFeatures;
        preLoadConfiguration();
    }

    public static ModifiableConfiguration buildGraphConfiguration() {
        return new ModifiableConfiguration(ROOT_NS,
                new CommonsConfiguration(new BaseConfiguration()),
                BasicConfiguration.Restriction.NONE);
    }

    public boolean hasFlushIDs() {
        return flushIDs;
    }

    public boolean hasForceIndexUsage() {
        return forceIndexUsage;
    }

    public int getTxVertexCacheSize() {
        return txVertexCacheSize;
    }

    public int getTxDirtyVertexSize() {
        return txDirtyVertexSize;
    }

    public boolean isBatchLoading() {
        return batchLoading;
    }

    public String getUniqueGraphId() {
        return uniqueGraphId;
    }

    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    public DefaultSchemaMaker getDefaultSchemaMaker() {
        return defaultSchemaMaker;
    }

    public Duration getMaxCommitTime() {
        return configuration.get(MAX_COMMIT_TIME);
    }

    public Duration getMaxWriteTime() {
        return configuration.get(STORAGE_WRITE_WAITTIME);
    }

    public boolean hasPropertyPrefetching() {
        if (propertyPrefetching == null) {
            return getStoreFeatures().isDistributed();
        } else {
            return propertyPrefetching;
        }
    }

    public boolean useMultiQuery() {
        return useMultiQuery;
    }

    public boolean batchPropertyPrefetching() {
        return batchPropertyPrefetching;
    }

    public boolean adjustQueryLimit() {
        return adjustQueryLimit;
    }

    public String getUnknownIndexKeyName() {
        return unknownIndexKeyName;
    }

    public boolean hasLogTransactions() {
        return logTransactions;
    }

    public TimestampProvider getTimestampProvider() {
        return configuration.get(TIMESTAMP_PROVIDER);
    }

    public boolean isUpgradeAllowed(String name) {
        return configuration.get(ALLOW_UPGRADE);
    }

    public String getBackendDescription() {
        String className = configuration.get(STORAGE_BACKEND);
        if (className.equalsIgnoreCase("berkeleyje")) {
            return className + ":" + configuration.get(STORAGE_DIRECTORY);
        } else {
            return className + ":" + Arrays.toString(configuration.get(STORAGE_HOSTS));
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public Serializer getSerializer() {
        return getSerializer(configuration);
    }

    private static Serializer getSerializer(Configuration configuration) {
        Serializer serializer = new StandardSerializer();

        List<RegisteredAttributeClass<?>> registeredAttributeClasses = RegisteredAttributeClassesConverter.getInstance().convert(configuration);

        for (RegisteredAttributeClass<?> clazz : registeredAttributeClasses) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public SchemaCache getTypeCache(SchemaCache.StoreRetrieval retriever) {
        if (configuration.get(BASIC_METRICS)) return new MetricInstrumentedSchemaCache(retriever);
        else return new StandardSchemaCache(retriever);
    }

    public org.apache.commons.configuration.Configuration getConfigurationAtOpen() {
        return ReadConfigurationConverter.getInstance().convert(configurationAtOpen);
    }

    private void preLoadConfiguration() {
        flushIDs = configuration.get(IDS_FLUSH);
        forceIndexUsage = configuration.get(FORCE_INDEX_USAGE);
        batchLoading = configuration.get(STORAGE_BATCH);

        //Disable auto-type making when batch-loading is enabled since that may overwrite types without warning
        if (batchLoading) {
            defaultSchemaMaker = new DisableDefaultSchemaMaker();
        } else {
            defaultSchemaMaker = new JanusGraphDefaultSchemaMaker();
        }

        txVertexCacheSize = configuration.get(TX_CACHE_SIZE);
        //Check for explicit dirty vertex cache size first, then fall back on batch-loading-dependent default
        if (configuration.has(TX_DIRTY_SIZE)) {
            txDirtyVertexSize = configuration.get(TX_DIRTY_SIZE);
        } else {
            txDirtyVertexSize = batchLoading ? TX_DIRTY_SIZE_DEFAULT_WITH_BATCH :
                    TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH;
        }

        propertyPrefetching = configuration.get(PROPERTY_PREFETCHING);
        useMultiQuery = configuration.get(USE_MULTIQUERY);
        batchPropertyPrefetching = configuration.get(BATCH_PROPERTY_PREFETCHING);
        adjustQueryLimit = configuration.get(ADJUST_LIMIT);
        logTransactions = configuration.get(SYSTEM_LOG_TRANSACTIONS);

        unknownIndexKeyName = configuration.get(IGNORE_UNKNOWN_INDEX_FIELD) ? UNKNOWN_FIELD_NAME : null;
    }
}

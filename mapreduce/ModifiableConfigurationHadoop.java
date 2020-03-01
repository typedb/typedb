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

package grakn.core.mapreduce;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.configuration.ConfigElement;
import grakn.core.graph.diskstorage.configuration.ConfigNamespace;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.diskstorage.util.time.Durations;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ModifiableConfigurationHadoop extends ModifiableConfiguration {

    // JanusGraph Hadoop I/O format configuration

    static final ConfigNamespace MAPRED_NS =
            new ConfigNamespace(null, "janusgraphmr", "JanusGraph MapReduce configuration root");
    private static final ConfigNamespace IOFORMAT_NS =
            new ConfigNamespace(MAPRED_NS, "ioformat", "JanusGraph input configuration");
    private static final ConfigNamespace GRAPH_CONFIG_KEYS =
            new ConfigNamespace(IOFORMAT_NS, "conf", "Settings to be passed to JanusGraphFactory.open");
    static final ConfigOption<String> COLUMN_FAMILY_NAME =
            new ConfigOption<>(IOFORMAT_NS, "cf-name",
                               "The name of the column family from which the Hadoop input format should read.  " +
                            "Usually edgestore or graphindex.", ConfigOption.Type.LOCAL, Backend.EDGESTORE_NAME);
    static final ConfigOption<Boolean> FILTER_PARTITIONED_VERTICES =
            new ConfigOption<>(IOFORMAT_NS, "filter-partitioned-vertices",
                    "True to drop partitioned vertices and relations incident on partitioned vertices when reading " +
                            "from JanusGraph.  This currently must be true when partitioned vertices are present in the " +
                            "input; if it is false when a partitioned vertex is encountered, then an exception is thrown.  " +
                            "This limitation may be lifted in a later version of JanusGraph-Hadoop.",
                    ConfigOption.Type.LOCAL, false);
    private final Configuration conf;

    private ModifiableConfigurationHadoop(ConfigNamespace root, Configuration c) {
        super(root, new WriteConfigurationHadoop(c));
        this.conf = c;
    }

    public static ModifiableConfigurationHadoop of(ConfigNamespace root, Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableConfigurationHadoop(root, c);
    }

    private static ModifiableConfiguration prefixView(ModifiableConfigurationHadoop mc) {
        WriteConfigurationHadoop prefixConf = new WriteConfigurationHadoop(mc.conf,
                                                                           ConfigElement.getPath(GRAPH_CONFIG_KEYS, true) + ".");
        return new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, prefixConf);
    }

    ModifiableConfiguration getJanusGraphConf() {
        return prefixView(this);
    }

    private static class WriteConfigurationHadoop implements WriteConfiguration {

        private static final Logger LOG = LoggerFactory.getLogger(WriteConfigurationHadoop.class);

        private final Configuration config;
        private final String prefix;

        WriteConfigurationHadoop(Configuration config) {
            this(config, null);
        }

        WriteConfigurationHadoop(Configuration config, String prefix) {
            this.config = config;
            this.prefix = prefix;
        }

        @Override
        public <O> O get(String key, Class<O> dataType) {

            String internalKey = getInternalKey(key);

            if (null == config.get(internalKey)) {
                return null;
            }

            if (dataType.isArray()) {
                Preconditions.checkArgument(dataType.getComponentType() == String.class, "Only string arrays are supported: %s", dataType);
                return (O) config.getStrings(internalKey);
            } else if (Number.class.isAssignableFrom(dataType)) {
                String s = config.get(internalKey);
                return constructFromStringArgument(dataType, s);
            } else if (dataType == String.class) {
                return (O) config.get(internalKey);
            } else if (dataType == Boolean.class) {
                return (O) Boolean.valueOf(config.get(internalKey));
            } else if (dataType.isEnum()) {
                O[] constants = dataType.getEnumConstants();
                Preconditions.checkState(null != constants && 0 < constants.length, "Zero-length or undefined enum");
                String estr = config.get(internalKey);
                for (O c : constants) {
                    if (c.toString().equals(estr)) {
                        return c;
                    }
                }
                throw new IllegalArgumentException("No match for string \"" + estr + "\" in enum " + dataType);
            } else if (dataType == Object.class) {
                // Return String when an Object is requested
                // Object.class must be supported for the sake of AbstractConfiguration's getSubset impl
                return (O) config.get(internalKey);
            } else if (Duration.class.isAssignableFrom(dataType)) {
                // This is a conceptual leak; the config layer should ideally only handle standard library types
                String s = config.get(internalKey);
                String[] comps = s.split("\\s");
                final TemporalUnit unit;
                switch (comps.length) {
                    case 1:
                        //By default, times are in milli seconds
                        unit = ChronoUnit.MILLIS;
                        break;
                    case 2:
                        unit = Durations.parse(comps[1]);
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot parse time duration from: " + s);
                }
                return (O) Duration.of(Long.valueOf(comps[0]), unit);
            } else throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        @Override
        public Iterable<String> getKeys(String userPrefix) {
            /*
             * Is there a way to iterate over just the keys of a Hadoop Configuration?
             * Iterating over Map.Entry is needlessly wasteful since we don't need the values.
             */

            return StreamSupport.stream(config.spliterator(), false)
                    .map(Map.Entry::getKey)
                    .filter(internalKey -> {
                        String k = internalKey;
                        if (null != prefix) {
                            if (k.startsWith(prefix)) {
                                k = getUserKey(k);
                            } else {
                                return false; // does not have the prefix
                            }
                        }
                        return k.startsWith(userPrefix);
                    })
                    .map(internalKey -> {
                        String userKey = getUserKey(internalKey);
                        Preconditions.checkState(userKey.startsWith(userPrefix));
                        return userKey;
                    })
                    .collect(Collectors.toList());
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public <O> void set(String key, O value) {

            String internalKey = getInternalKey(key);
            Class<?> dataType = value.getClass();

            if (dataType.isArray()) {
                Preconditions.checkArgument(dataType.getComponentType() == String.class, "Only string arrays are supported: %s", dataType);
                config.setStrings(internalKey, (String[]) value);
            } else if (Number.class.isAssignableFrom(dataType)) {
                config.set(internalKey, value.toString());
            } else if (dataType == String.class) {
                config.set(internalKey, value.toString());
            } else if (dataType == Boolean.class) {
                config.setBoolean(internalKey, (Boolean) value);
            } else if (dataType.isEnum()) {
                config.set(internalKey, value.toString());
            } else if (dataType == Object.class) {
                config.set(internalKey, value.toString());
            } else if (Duration.class.isAssignableFrom(dataType)) {
                // This is a conceptual leak; the config layer should ideally only handle standard library types
                String millis = String.valueOf(((Duration) value).toMillis());
                config.set(internalKey, millis);
            } else throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        @Override
        public void remove(String key) {
            config.unset(getInternalKey(key));
        }

        private <O> O constructFromStringArgument(Class<O> dataType, String arg) {
            try {
                Constructor<O> ctor = dataType.getConstructor(String.class);
                return ctor.newInstance(arg);
                // ReflectiveOperationException is narrower and more appropriate than Exception, but only @since 1.7
                //} catch (ReflectiveOperationException e) {
            } catch (Exception e) {
                LOG.error("Failed to parse configuration string \"{}\" into type {} due to the following reflection exception", arg, dataType, e);
                throw new RuntimeException(e);
            }
        }

        private String getInternalKey(String userKey) {
            return null == prefix ? userKey : prefix + userKey;
        }

        private String getUserKey(String internalKey) {
            String k = internalKey;

            if (null != prefix) {
                Preconditions.checkState(k.startsWith(prefix), "key %s does not start with prefix %s", internalKey, prefix);
                Preconditions.checkState(internalKey.length() > prefix.length());
                k = internalKey.substring(prefix.length());
            }

            return k;
        }
    }
}

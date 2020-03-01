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

package grakn.core.graph.diskstorage.configuration.backend;

import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.diskstorage.util.time.Durations;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * WriteConfiguration wrapper for Apache Configuration
 */
public class CommonsConfiguration implements WriteConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(CommonsConfiguration.class);

    private final Configuration config;

    public CommonsConfiguration() {
        this.config = new BaseConfiguration();
    }

    @Override
    public <O> O get(String key, Class<O> dataType) {
        if (!config.containsKey(key)) return null;

        if (dataType.isArray()) {
            if (dataType.getComponentType() != String.class) {
                throw new IllegalArgumentException("Only string arrays are supported: " + dataType);
            }
            return (O) config.getStringArray(key);
        } else if (Number.class.isAssignableFrom(dataType)) {
            // A properties file configuration returns Strings even for numeric
            // values small enough to fit inside Integer (e.g. 5000). In-memory
            // configuration implementations seem to be able to store and return actual
            // numeric types rather than String
            //
            // We try to handle either case here
            Object o = config.getProperty(key);
            if (dataType.isInstance(o)) {
                return (O) o;
            } else {
                return constructFromStringArgument(dataType, o.toString());
            }
        } else if (dataType == String.class) {
            return (O) config.getString(key);
        } else if (dataType == Boolean.class) {
            return (O) Boolean.valueOf(config.getBoolean(key));
        } else if (dataType.isEnum()) {
            Enum[] constants = (Enum[]) dataType.getEnumConstants();

            String enumString = config.getProperty(key).toString();
            for (Enum ec : constants) {
                if (ec.toString().equals(enumString)) {
                    return (O) ec;
                }
            }
            throw new IllegalArgumentException("No match for string \"" + enumString + "\" in enum " + dataType);
        } else if (dataType == Object.class) {
            return (O) config.getProperty(key);
        } else if (Duration.class.isAssignableFrom(dataType)) {
            // This is a conceptual leak; the config layer should ideally only handle standard library types
            Object o = config.getProperty(key);
            if (o instanceof Duration) {
                return (O) o;
            } else {
                String[] comps = o.toString().split("\\s");
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
                        throw new IllegalArgumentException("Cannot parse time duration from: " + o.toString());
                }
                return (O) Duration.of(Long.valueOf(comps[0]), unit);
            }
        } else throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    private <O> O constructFromStringArgument(Class<O> dataType, String arg) {
        try {
            Constructor<O> ctor = dataType.getConstructor(String.class);
            return ctor.newInstance(arg);
        } catch (Exception e) {
            LOG.error("Failed to parse configuration string \"{}\" into type {} due to the following reflection exception", arg, dataType, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        List<String> result = new ArrayList<>();
        Iterator<String> keys;
        if (StringUtils.isNotBlank(prefix)) keys = config.getKeys(prefix);
        else keys = config.getKeys();
        while (keys.hasNext()) result.add(keys.next());
        return result;
    }

    @Override
    public void close() {
        //Do nothing
    }

    @Override
    public <O> void set(String key, O value) {
        if (value == null) {
            config.clearProperty(key);
        } else if (Duration.class.isAssignableFrom(value.getClass())) {
            config.setProperty(key, ((Duration) value).toMillis());
        } else {
            config.setProperty(key, value);
        }
    }

    @Override
    public void remove(String key) {
        config.clearProperty(key);
    }

}

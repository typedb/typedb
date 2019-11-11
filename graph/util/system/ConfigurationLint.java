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

package grakn.core.graph.util.system;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import grakn.core.graph.diskstorage.configuration.ConfigElement;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class ConfigurationLint {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationLint.class);

    public static void main(String args[]) throws IOException {
        if (1 != args.length) {
            System.err.println("Usage: ConfigurationLint janusgraph.properties");
            System.err.println("  Reads the supplied config file from disk and checks for unknown options.");
            System.exit(1);
        }

        LOG.info("Checking " + LoggerUtil.sanitizeAndLaunder(args[0]));
        Status s = validate(args[0]);
        if (0 == s.errors) {
            LOG.info(s.toString());
        } else {
            LOG.warn(s.toString());
        }
        System.exit(Math.min(s.errors, 127));
    }

    public static Status validate(String filename) throws IOException {
        try (FileInputStream fis = new FileInputStream(filename)) {
            new Properties().load(fis);
        }

        PropertiesConfiguration apc;
        try {
            apc = new PropertiesConfiguration(filename);
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }


        Iterator<String> iterator = apc.getKeys();

        int totalKeys = 0;
        int keysVerified = 0;

        while (iterator.hasNext()) {
            totalKeys++;
            String key = iterator.next();
            String value = apc.getString(key);
            try {
                ConfigElement.PathIdentifier pid = ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, key);
                // ConfigElement shouldn't return null; failure here probably relates to janusgraph-core, not the file
                Preconditions.checkNotNull(pid);
                Preconditions.checkNotNull(pid.element);
                if (!pid.element.isOption()) {
                    LOG.warn("Config key {} is a namespace (only options can be keys)", key);
                    continue;
                }
                final ConfigOption<?> opt;
                try {
                    opt = (ConfigOption<?>) pid.element;
                } catch (RuntimeException re) {
                    // This shouldn't happen given the preceding check, but catch it anyway
                    LOG.warn("Config key {} maps to the element {}, but it could not be cast to an option",
                            key, pid.element, re);
                    continue;
                }
                try {
                    Object o = new CommonsConfiguration(apc).get(key, opt.getDatatype());
                    opt.verify(o);
                    keysVerified++;
                } catch (RuntimeException re) {
                    LOG.warn("Config key {} is recognized, but its value {} could not be validated",
                            key, value /*, re*/);
                    LOG.debug("Validation exception on {}={} follows", key, value, re);
                }
            } catch (RuntimeException re) {
                LOG.warn("Unknown config key {}", key);
            }
        }


        return new Status(totalKeys, totalKeys - keysVerified);
    }

    public static class Status {
        private final int total;
        private final int errors;

        public Status(int total, int errors) {
            this.total = total;
            this.errors = errors;
        }

        public int getTotalSettingCount() {
            return total;
        }

        public int getErrorSettingCount() {
            return errors;
        }

        public String toString() {
            if (0 == errors) {
                return String.format("[ConfigurationLint.Status: OK: %d settings validated]", total);
            } else {
                return String.format("[ConfigurationLint.Status: WARNING: %d settings failed to validate out of %d total]", errors, total);
            }
        }
    }
}

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

package grakn.core.graph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read-only configuration that can access configurations using namespace
 */
public class BasicConfiguration implements Configuration {

    private final ConfigNamespace root;
    private static final Logger LOG = LoggerFactory.getLogger(BasicConfiguration.class);
    private final ReadConfiguration config;

    public BasicConfiguration(ConfigNamespace root, ReadConfiguration config) {
        Preconditions.checkNotNull(root);
        Preconditions.checkArgument(!root.isUmbrella(), "Root cannot be an umbrella namespace");
        Preconditions.checkNotNull(config);

        this.root = root;
        this.config = config;
    }

    @Override
    public boolean has(ConfigOption option, String... umbrellaElements) {
        return config.get(getPath(option, umbrellaElements), option.getDatatype()) != null;
    }

    @Override
    public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
        O result = config.get(getPath(option, umbrellaElements), option.getDatatype());
        return option.get(result);
    }

    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
        return getContainedNamespaces(config, umbrella, umbrellaElements);
    }

    @Override
    public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
        return getSubset(config, umbrella, umbrellaElements);
    }

    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return restrictTo(this, umbrellaElements);
    }

    public Map<ConfigElement.PathIdentifier, Object> getAll() {
        Map<ConfigElement.PathIdentifier, Object> result = new HashMap<>();

        for (String key : config.getKeys("")) {
            Preconditions.checkArgument(StringUtils.isNotBlank(key));
            try {
                ConfigElement.PathIdentifier pid = ConfigElement.parse(root, key);
                Preconditions.checkArgument(pid.element.isOption() && !pid.lastIsUmbrella);
                result.put(pid, get((ConfigOption) pid.element, pid.umbrellaElements));
            } catch (IllegalArgumentException e) {
                LOG.debug("Ignored configuration entry for {} since it does not map to an option", key, e);
            }
        }
        return result;
    }

    public ReadConfiguration getConfiguration() {
        return config;
    }


    public void close() {
        config.close();
    }

    private void verifyElement(ConfigElement element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(element.getRoot().equals(root), "Configuration element is not associated with this configuration: %s", element);
    }

    protected String getPath(ConfigElement option, String... umbrellaElements) {
        verifyElement(option);
        return ConfigElement.getPath(option, umbrellaElements);
    }

    private Set<String> getContainedNamespaces(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);
        Preconditions.checkArgument(umbrella.isUmbrella());

        String prefix = ConfigElement.getPath(umbrella, umbrellaElements);
        Set<String> result = new HashSet<>();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix));
            String sub = key.substring(prefix.length() + 1).trim();
            if (!sub.isEmpty()) {
                String ns = ConfigElement.getComponents(sub)[0];
                Preconditions.checkArgument(StringUtils.isNotBlank(ns), "Invalid sub-namespace for key: %s", key);
                result.add(ns);
            }
        }
        return result;
    }

    private Map<String, Object> getSubset(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);

        String prefix = umbrella.isRoot() ? "" : ConfigElement.getPath(umbrella, umbrellaElements);
        Map<String, Object> result = new HashMap<>();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix));
            // A zero-length prefix is a root.  A positive-length prefix
            // is not a root and we should tack on an additional character
            // to consume the dot between the prefix and the rest of the key.
            int startIndex = umbrella.isRoot() ? prefix.length() : prefix.length() + 1;
            String sub = key.substring(startIndex).trim();
            if (!sub.isEmpty()) {
                result.put(sub, config.get(key, Object.class));
            }
        }
        return result;
    }

    /**
     * Return a new Configuration which contains a subset of current BasicConfiguration
     */
    private static Configuration restrictTo(Configuration config, String... fixedUmbrella) {
        Preconditions.checkArgument(fixedUmbrella != null && fixedUmbrella.length > 0);
        return new Configuration() {

            private String[] concat(String... others) {
                if (others == null || others.length == 0) return fixedUmbrella;
                String[] join = new String[fixedUmbrella.length + others.length];
                System.arraycopy(fixedUmbrella, 0, join, 0, fixedUmbrella.length);
                System.arraycopy(others, 0, join, fixedUmbrella.length, others.length);
                return join;
            }

            @Override
            public boolean has(ConfigOption option, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella()) {
                    return config.has(option, concat(umbrellaElements));
                } else {
                    return config.has(option);
                }
            }

            @Override
            public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella()) {
                    return config.get(option, concat(umbrellaElements));
                } else {
                    return config.get(option);
                }
            }

            @Override
            public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
                return config.getContainedNamespaces(umbrella, concat(umbrellaElements));
            }

            @Override
            public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
                return config.getSubset(umbrella, concat(umbrellaElements));
            }

            @Override
            public Configuration restrictTo(String... umbrellaElements) {
                return config.restrictTo(concat(umbrellaElements));
            }
        };
    }

}

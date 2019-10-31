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

package grakn.core.graph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Read-only configuration that can be optionally restricted to only accept LOCAL or GLOBAL options.
 */
public class BasicConfiguration implements Configuration {

    private final ConfigNamespace root;
    private static final Logger LOG = LoggerFactory.getLogger(org.janusgraph.diskstorage.configuration.BasicConfiguration.class);

    public enum Restriction {LOCAL, GLOBAL, NONE}

    private final ReadConfiguration config;
    private final Restriction restriction;

    public BasicConfiguration(ConfigNamespace root, ReadConfiguration config, Restriction restriction) {
        Preconditions.checkNotNull(root);
        Preconditions.checkArgument(!root.isUmbrella(), "Root cannot be an umbrella namespace");
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(restriction);

        this.root = root;
        this.config = config;
        this.restriction = restriction;
    }

    ConfigNamespace getRootNamespace() {
        return root;
    }

    void verifyOption(ConfigOption option) {
        Preconditions.checkNotNull(option);
        verifyElement(option);
        if (restriction == Restriction.GLOBAL) {
            Preconditions.checkArgument(option.isGlobal(), "Can only accept global options: %s", option);
        } else if (restriction == Restriction.LOCAL) {
            Preconditions.checkArgument(option.isLocal(), "Can only accept local options: %s", option);
        }
    }

    @Override
    public boolean has(ConfigOption option, String... umbrellaElements) {
        verifyOption(option);
        return config.get(getPath(option, umbrellaElements), option.getDatatype()) != null;
    }

    @Override
    public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
        verifyOption(option);
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
        Map<ConfigElement.PathIdentifier, Object> result = Maps.newHashMap();

        for (String key : config.getKeys("")) {
            Preconditions.checkArgument(StringUtils.isNotBlank(key));
            try {
                final ConfigElement.PathIdentifier pid = ConfigElement.parse(getRootNamespace(), key);
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
        Set<String> result = Sets.newHashSet();

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

    protected Map<String, Object> getSubset(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);

        String prefix = umbrella.isRoot() ? "" : ConfigElement.getPath(umbrella, umbrellaElements);
        Map<String, Object> result = Maps.newHashMap();

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
                if (option.getNamespace().hasUmbrella())
                    return config.has(option, concat(umbrellaElements));
                else
                    return config.has(option);
            }

            @Override
            public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella())
                    return config.get(option, concat(umbrellaElements));
                else
                    return config.get(option);
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
